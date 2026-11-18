/**
 * Cloudflare Worker - IMDb Ratings Proxy with D1 Cache & Smart TTL
 * Optimized for Batched Subrequests
 */

const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
};

const OMDB_BASE_URL = 'https://www.omdbapi.com';

const TTL = {
    RECENT: 60 * 60 * 1000,           // 1 hour
    MEDIUM: 24 * 60 * 60 * 1000,      // 1 day
    STABLE: 30 * 24 * 60 * 60 * 1000  // 30 days
};

// OMDb daily quota handling. Once OMDb says "Request limit reached!", skip all
// OMDb calls in this isolate for a while instead of burning more refused calls.
const RATE_LIMIT_BACKOFF_MS = 10 * 60 * 1000; // 10 minutes

// Titles OMDb couldn't match are remembered for this long (not_found table),
// so they aren't re-searched by every user on every page view.
const NOT_FOUND_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
let omdbLimitedUntil = 0;

class OmdbRateLimitError extends Error {
    constructor() {
        super('OMDb rate limit reached');
        this.name = 'OmdbRateLimitError';
    }
}

/**
 * Single entry point for every OMDb request: enforces the backoff and turns
 * OMDb's limit response into an error (instead of looking like "not found").
 */
async function omdbGet(params) {
    if (Date.now() < omdbLimitedUntil) throw new OmdbRateLimitError();
    const res = await fetch(`${OMDB_BASE_URL}/?${params}`);
    const data = await res.json();
    if (data.Response !== 'True' && /limit/i.test(data.Error || '')) {
        omdbLimitedUntil = Date.now() + RATE_LIMIT_BACKOFF_MS;
        throw new OmdbRateLimitError();
    }
    return data;
}

export default {
    async fetch(request, env, ctx) {
        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: corsHeaders });
        }

        if (request.method !== 'POST') {
            return jsonResponse({ error: 'Method not allowed' }, 405);
        }

        const url = new URL(request.url);

        if (url.pathname.endsWith('/batch')) {
            return handleBatchLookup(request, env);
        }

        return jsonResponse({ error: 'Not found' }, 404);
    }
};

/**
 * Handle batch movie lookups
 */
async function handleBatchLookup(request, env) {
    try {
        const body = await request.json();
        const { movies } = body;

        if (!movies || !Array.isArray(movies)) {
            return jsonResponse({ error: 'Invalid payload' }, 400);
        }

        // 1. Batch D1 Lookup
        const hrefs = [...new Set(movies.map(m => m.href).filter(Boolean))];
        let cachedMap = new Map();
        const notFoundMap = new Map(); // href -> checked_at

        if (hrefs.length > 0) {
            const placeholders = hrefs.map(() => '?').join(',');
            const cachedRows = await env.DB.prepare(`SELECT * FROM movies WHERE prime_href IN (${placeholders})`)
                .bind(...hrefs)
                .all();

            if (cachedRows.results) {
                cachedRows.results.forEach(row => {
                    cachedMap.set(row.prime_href, row);
                });
            }

            // Recently-missed titles (table may not exist yet — fail open)
            try {
                const missRows = await env.DB.prepare(`SELECT prime_href, checked_at FROM not_found WHERE prime_href IN (${placeholders})`)
                    .bind(...hrefs)
                    .all();
                (missRows.results || []).forEach(row => notFoundMap.set(row.prime_href, row.checked_at));
            } catch (e) {
                console.error('not_found lookup failed:', e.message);
            }
        }

        // 2. Process movies (Cache check -> OMDb fetch if needed)
        const updates = [];
        const settled = await Promise.allSettled(
            movies.map(async (movie) => {
                const cached = cachedMap.get(movie.href);
                const result = await processMovieLogic(movie, cached, env, notFoundMap.get(movie.href));

                // Save fresh lookups AND TTL refreshes (refreshes were previously
                // never saved, so stale rows re-hit OMDb on every view)
                if ((result.source === 'api' || result.source === 'api-refresh') && result.data) {
                    updates.push(result.data);
                }
                return result;
            })
        );
        const results = settled.map((s, i) =>
            s.status === 'fulfilled' ? s.value : { href: movies[i].href, error: s.reason?.message || 'Unknown error' }
        );

        // 3. Batch D1 Saves
        if (updates.length > 0) {
            const statements = updates.map(data => {
                return env.DB.prepare(`
                    INSERT INTO movies (imdb_id, title, year, release_date, prime_href, rating, rt_rating, votes, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(prime_href) DO UPDATE SET
                        imdb_id = excluded.imdb_id,
                        rating = excluded.rating,
                        rt_rating = excluded.rt_rating,
                        votes = excluded.votes,
                        updated_at = excluded.updated_at
                `).bind(
                    data.imdb_id,
                    data.title,
                    data.year,
                    data.release_date,
                    data.prime_href,
                    data.rating,
                    data.rt_rating,
                    data.votes,
                    data.updated_at
                );
            });
            await env.DB.batch(statements);
        }

        // 4. Remember misses / clear misses that now resolved
        const missStatements = [];
        const now = new Date().toISOString();
        results.forEach(r => {
            if (r.notFound) {
                missStatements.push(env.DB.prepare(
                    `INSERT INTO not_found (prime_href, checked_at) VALUES (?, ?)
                     ON CONFLICT(prime_href) DO UPDATE SET checked_at = excluded.checked_at`
                ).bind(r.href, now));
            }
        });
        updates.forEach(data => {
            if (notFoundMap.has(data.prime_href)) {
                missStatements.push(env.DB.prepare(`DELETE FROM not_found WHERE prime_href = ?`).bind(data.prime_href));
            }
        });
        if (missStatements.length > 0) {
            try {
                await env.DB.batch(missStatements);
            } catch (e) {
                console.error('not_found write failed:', e.message);
            }
        }

        return jsonResponse({ results });
    } catch (error) {
        console.error('Batch lookup error:', error);
        return jsonResponse({ error: 'Internal server error', details: error.message }, 500);
    }
}

async function processMovieLogic(movie, cached, env, notFoundAt) {
    let { title, href, entityType, verificationRating, year } = movie;
    if (!href) return { href, error: 'Missing href' };

    // Standardize year
    if (year) year = parseInt(year);
    const omdbType = omdbTypeFor(entityType);

    // Recently searched with no match — skip OMDb. Detail pages (which send
    // Prime's rating + year) always search, since they can succeed where a
    // home-card search failed.
    if (!cached && notFoundAt && !verificationRating &&
        Date.now() - new Date(notFoundAt).getTime() < NOT_FOUND_TTL_MS) {
        return { href, error: 'OMDb Not Found', source: 'not-found-cache' };
    }

    if (cached) {
        if (!isDataStale(cached.release_date, cached.updated_at)) {
            // Check verification mismatch if provided
            if (verificationRating) {
                const cachedRating = parseFloat(cached.rating);
                const localRating = parseFloat(verificationRating);
                // If cached rating differs significantly from local rating, invalidate cache
                if (Math.abs(cachedRating - localRating) >= 0.2) {
                    // Proceed to API fetch (fall through)
                    // We don't mark as 'cache' yet, we fall through to API fetch
                } else {
                    return { href, data: cached, source: 'cache', debug: 'verification-pass' };
                }
            } else {
                return { href, data: cached, source: 'cache' };
            }
        } else {
            // TTL expired - refresh using cached imdb_id if available
            if (cached.imdb_id) {
                try {
                    const refreshed = await fetchOMDbById(cached.imdb_id, env.OMDB_API_KEY);
                    if (refreshed && refreshed.imdbRating && refreshed.imdbRating !== 'N/A') {
                        const rtRating = extractRottenTomatoes(refreshed.Ratings);
                        const movieData = {
                            imdb_id: refreshed.imdbID,
                            title: cached.title,
                            year: parseInt(refreshed.Year) || cached.year,
                            release_date: parseReleaseDate(refreshed.Released) || cached.release_date,
                            prime_href: href,
                            rating: parseFloat(refreshed.imdbRating) || cached.rating,
                            rt_rating: rtRating || cached.rt_rating,
                            votes: parseInt((refreshed.imdbVotes || '0').replace(/,/g, '')) || cached.votes,
                            updated_at: new Date().toISOString()
                        };
                        return { href, data: movieData, source: 'api-refresh' };
                    }
                } catch (e) {
                    if (!(e instanceof OmdbRateLimitError)) {
                        console.error(`[TTL Refresh Error] ID lookup failed for ${cached.imdb_id}:`, e);
                    }
                }
                // If ID lookup fails, return stale cache rather than doing title search
                return { href, data: cached, source: 'cache-stale' };
            }
        }

        if (!title && !cached.title) return { href, data: cached, source: 'cache-stale' };
        if (!title) title = cached.title; // Use cached title if missing from payload
        if (!year && cached.year) year = cached.year; // Use cached year if available
    }

    if (!title) {
        return { href, error: 'OMDb Lookup skipped: Missing title' };
    }

    // Pre-processing
    const directorsCutRegex = /\s*Director's Cut$/i;
    if (directorsCutRegex.test(title) && title.replace(directorsCutRegex, '').trim().length > 0) {
        title = title.replace(directorsCutRegex, '').trim();
    }
    title = title.replace(/\s*(?:\([^)]*\)|\[[^\]]*\])\s*$/, '').trim() || title;

    try {
        let omdbResult = null;

        // Step 1: Try title lookup with cleaned title AND year if available
        omdbResult = await fetchOMDbTitle(title, year, env.OMDB_API_KEY, omdbType);

        // Validation
        if (omdbResult) {
            const isValid = isValidResult(omdbResult, entityType);
            if (!isValid) omdbResult = null;
        }

        // Verification Check Logic
        // If we have a verification rating (Detail Page), and the current result mismatches or is null
        let needsFallback = false;
        if (verificationRating) {
            if (!omdbResult || !omdbResult.imdbRating || omdbResult.imdbRating === 'N/A' ||
                Math.abs(parseFloat(omdbResult.imdbRating) - parseFloat(verificationRating)) >= 0.2) {

                needsFallback = true;
                // Discard bad result if it was a mismatch
                omdbResult = null;
            }
        } else {
            // Normal logic: if no result, need fallback
            if (!omdbResult) needsFallback = true;
        }

        if (needsFallback) {
            // Step 2: Ampersand replacement (Common fallback)
            if (title.includes('&')) {
                const altTitle = title.replace(/&/g, ' and ').replace(/\s+/g, ' ').trim();
                omdbResult = await fetchOMDbTitle(altTitle, year, env.OMDB_API_KEY, omdbType);
                if (omdbResult && !isValidResult(omdbResult, entityType)) omdbResult = null;
            }
        }

        // Step 3: Search API Fallback
        if (!omdbResult && needsFallback) {
            omdbResult = await fetchOMDbSearch(title, year, env.OMDB_API_KEY, omdbType);
            if (omdbResult && !isValidResult(omdbResult, entityType)) omdbResult = null;
        }

        // Step 4: Special Character Truncation (The Critical Split Logic)
        if (!omdbResult || (verificationRating && Math.abs(parseFloat(omdbResult.imdbRating) - parseFloat(verificationRating)) >= 0.2)) {
            const specialCharMatch = title.match(/[-:&]/);
            if (specialCharMatch) {
                if (verificationRating) {
                    // Reverse Fallback for Verification Mismatch
                    const reverseTitle = title.substring(specialCharMatch.index + 1).trim();
                    if (reverseTitle.length > 2) {
                        omdbResult = await fetchOMDbTitle(reverseTitle, year, env.OMDB_API_KEY, omdbType);
                        if (!omdbResult) {
                            omdbResult = await fetchOMDbSearch(reverseTitle, year, env.OMDB_API_KEY, omdbType);
                        }
                    }
                } else {
                    // Standard Fallback
                    const truncatedTitle = title.substring(0, specialCharMatch.index).trim();
                    if (truncatedTitle.length > 2) {
                        omdbResult = await fetchOMDbSearch(truncatedTitle, year, env.OMDB_API_KEY, omdbType);
                    }
                }
                if (omdbResult && !isValidResult(omdbResult, entityType)) omdbResult = null;
            }
        }

        if (!omdbResult) return { href, error: 'OMDb Not Found', notFound: true };

        const rtRating = extractRottenTomatoes(omdbResult.Ratings);

        const movieData = {
            imdb_id: omdbResult.imdbID,
            title: title,
            year: parseInt(omdbResult.Year) || 0,
            release_date: parseReleaseDate(omdbResult.Released),
            prime_href: href,
            rating: parseFloat(omdbResult.imdbRating) || 0,
            rt_rating: rtRating,
            votes: parseInt((omdbResult.imdbVotes || '0').replace(/,/g, '')) || 0,
            updated_at: new Date().toISOString()
        };

        return { href, data: movieData, source: 'api' };
    } catch (e) {
        if (e instanceof OmdbRateLimitError) {
            // Not a real "not found" — tell the client to retry later
            return { href, error: e.message, rateLimited: true };
        }
        console.error(`[Error] processing "${title}":`, e);
        return { href, error: e.message };
    }
}

/**
 * Map Prime's entity type to OMDb's `type` param ("Movie" -> movie, "TV Show" -> series).
 * Returns null for anything else so the param is omitted.
 */
function omdbTypeFor(primeEntityType) {
    if (primeEntityType === 'Movie') return 'movie';
    if (primeEntityType === 'TV Show') return 'series';
    return null;
}

/**
 * Validate OMDb result: type must match Prime entity type AND rating must not be N/A
 */
function isValidResult(omdbResult, primeEntityType) {
    // Check rating - must not be N/A
    if (!omdbResult.imdbRating || omdbResult.imdbRating === 'N/A') {
        console.log(`[Validation Fail] "${omdbResult.Title}" has N/A rating`);
        return false;
    }

    // Check type match if we have Prime entity type
    if (primeEntityType) {
        const omdbType = omdbResult.Type?.toLowerCase();
        // Prime: "Movie" -> OMDb: "movie"
        // Prime: "TV Show" -> OMDb: "series"
        const expectedOmdbType = primeEntityType === 'Movie' ? 'movie' : 'series';

        // Loose check: only enforce if both are present
        if (omdbType && expectedOmdbType && omdbType !== expectedOmdbType) {
            return false;
        }
    }

    return true;
}

function isDataStale(releaseDateStr, updatedAtStr) {
    if (!releaseDateStr) return true;
    const now = Date.now();
    const lastUpdate = new Date(updatedAtStr).getTime();
    const release = new Date(releaseDateStr).getTime();
    if (isNaN(lastUpdate) || isNaN(release)) return true;
    const daysSinceRelease = (now - release) / (1000 * 60 * 60 * 24);
    const timeSinceUpdate = now - lastUpdate;
    if (daysSinceRelease <= 7) return timeSinceUpdate > TTL.RECENT;
    if (daysSinceRelease <= 14) return timeSinceUpdate > TTL.MEDIUM;
    return timeSinceUpdate > TTL.STABLE;
}

/**
 * OMDb Title Lookup (exact match)
 */
async function fetchOMDbTitle(title, year, apiKey, type) {
    const params = new URLSearchParams({ apikey: apiKey, t: title });
    if (year) params.append('y', year);
    if (type) params.append('type', type);
    const data = await omdbGet(params);
    return data.Response === 'True' ? data : null;
}

/**
 * OMDb ID Lookup (direct by IMDb ID - most reliable)
 */
async function fetchOMDbById(imdbId, apiKey) {
    const params = new URLSearchParams({ apikey: apiKey, i: imdbId });
    const data = await omdbGet(params);
    return data.Response === 'True' ? data : null;
}

/**
 * OMDb Search API - returns first result with full details
 */
async function fetchOMDbSearch(title, year, apiKey, type) {
    const params = new URLSearchParams({ apikey: apiKey, s: title });
    if (year) params.append('y', year);
    if (type) params.append('type', type);
    const data = await omdbGet(params);

    if (data.Response !== 'True' || !data.Search || data.Search.length === 0) {
        return null;
    }

    // Get the first result's IMDb ID and fetch full details
    const firstResult = data.Search[0];
    const detailParams = new URLSearchParams({ apikey: apiKey, i: firstResult.imdbID });
    const detailData = await omdbGet(detailParams);

    return detailData.Response === 'True' ? detailData : null;
}

function parseReleaseDate(dateStr) {
    if (!dateStr || dateStr === 'N/A') return new Date().toISOString();
    return new Date(dateStr).toISOString();
}

function jsonResponse(data, status = 200) {
    return new Response(JSON.stringify(data), { status, headers: corsHeaders });
}

function extractRottenTomatoes(ratings) {
    if (!ratings || !Array.isArray(ratings)) return null;
    const rt = ratings.find(r => r.Source === 'Rotten Tomatoes');
    if (rt && rt.Value) {
        return rt.Value; // Return string "98%"
    }
    return null;
}

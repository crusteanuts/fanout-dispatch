const DEFAULT_DISPATCHER_COUNT = 4;
const DEFAULT_DISPATCHER_CONCURRENCY = 5;
const DEFAULT_DISPATCHER_BATCH_SIZE = 10;

export default {
    async fetch(request, env, ctx) {
        if (request.method !== "POST") {
            return new Response("Method Not Allowed", {
                status: 405,
            });
        }

        let body;

        try {
            body = await request.json();
        } catch {
            return new Response("Invalid JSON", {
                status: 400,
            });
        }

        /*
         * Dispatcher URL can come from the POST body.
         *
         * Optional fallback:
         * ?dispatcher=https://...
         */
        const dispatcherUrl =
            typeof body.dispatcherUrl === "string" &&
                body.dispatcherUrl.trim()
                ? body.dispatcherUrl.trim()
                : new URL(request.url)
                    .searchParams
                    .get("dispatcher");

        if (!dispatcherUrl) {
            return new Response("Missing dispatcherUrl", {
                status: 400,
            });
        }

        const workers = body.workers;
        const candidates = body.candidates;

        const method = String(
            body.method ?? "GET"
        ).toUpperCase();

        const headers =
            body.headers &&
                typeof body.headers === "object" &&
                !Array.isArray(body.headers)
                ? body.headers
                : {};

        /*
         * Use POST values when supplied, otherwise use defaults.
         */
        const dispatcherCount =
            Number.isInteger(body.dispatcherCount) &&
                body.dispatcherCount > 0
                ? body.dispatcherCount
                : DEFAULT_DISPATCHER_COUNT;

        const dispatcherConcurrency =
            Number.isInteger(body.dispatcherConcurrency) &&
                body.dispatcherConcurrency > 0
                ? body.dispatcherConcurrency
                : DEFAULT_DISPATCHER_CONCURRENCY;

        const dispatcherBatchSize =
            Number.isInteger(body.dispatcherBatchSize) &&
                body.dispatcherBatchSize > 0
                ? body.dispatcherBatchSize
                : DEFAULT_DISPATCHER_BATCH_SIZE;

        if (!Array.isArray(workers) || workers.length === 0) {
            return new Response("Invalid workers", {
                status: 400,
            });
        }

        if (!Array.isArray(candidates)) {
            return new Response("Invalid candidates", {
                status: 400,
            });
        }

        if (candidates.length === 0) {
            return new Response("[]", {
                status: 200,
                headers: {
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                },
            });
        }

        /*
         * ------------------------------------------------------------
         * Split candidates across Dispatcher requests.
         *
         * Example with defaults:
         *
         * 200 candidates
         *   -> 4 Dispatcher requests
         *   -> 50 candidates each
         *   -> concurrency 5
         *   -> batch size 10
         * ------------------------------------------------------------
         */

        const chunkSize = Math.ceil(
            candidates.length / dispatcherCount
        );

        const dispatcherTasks = [];

        for (
            let start = 0;
            start < candidates.length;
            start += chunkSize
        ) {
            const dispatcherCandidates = candidates.slice(
                start,
                start + chunkSize
            );

            dispatcherTasks.push(
                fetch(dispatcherUrl, {
                    method: "POST",

                    headers: {
                        "Content-Type": "application/json",
                    },

                    body: JSON.stringify({
                        workers,
                        candidates: dispatcherCandidates,
                        method,
                        headers,

                        concurrency: dispatcherConcurrency,
                        batch_size: dispatcherBatchSize,
                    }),
                })
                    .then(async response => {
                        if (!response.ok) {
                            const errorText =
                                await response.text();

                            throw new Error(
                                `Dispatcher HTTP ${response.status}: ${errorText}`
                            );
                        }

                        const data =
                            await response.json();

                        if (!Array.isArray(data)) {
                            throw new Error(
                                "Dispatcher returned invalid result"
                            );
                        }

                        return {
                            candidates:
                                dispatcherCandidates,
                            results: data,
                            error: null,
                        };
                    })
                    .catch(error => {
                        return {
                            candidates:
                                dispatcherCandidates,
                            results: null,
                            error:
                                error instanceof Error
                                    ? error.message
                                    : String(error),
                        };
                    })
            );
        }

        /*
         * All Dispatcher requests run concurrently.
         */
        const dispatcherResults =
            await Promise.all(dispatcherTasks);

        /*
         * Combine all Dispatcher results.
         */
        const results = [];

        for (const dispatcherResult of dispatcherResults) {
            if (dispatcherResult.results) {
                results.push(
                    ...dispatcherResult.results
                );

                continue;
            }

            /*
             * Preserve candidate-level errors if one Dispatcher
             * failed instead of failing the entire Fan-Out request.
             */
            for (
                const candidate of
                dispatcherResult.candidates
            ) {
                results.push({
                    key: candidate.key,
                    url: candidate.url,
                    worker: "",
                    result: null,
                    error: dispatcherResult.error,
                });
            }
        }

        return new Response(
            JSON.stringify(results),
            {
                status: 200,
                headers: {
                    "Content-Type":
                        "application/json",
                    "Cache-Control":
                        "no-cache",
                },
            }
        );
    },
};
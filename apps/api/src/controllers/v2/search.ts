import { Response } from "express";
import {
  Document,
  RequestWithAuth,
  SearchRequest,
  SearchResponse,
  searchRequestSchema,
  ScrapeOptions,
  TeamFlags,
} from "./types";
import { billTeam } from "../../services/billing/credit_billing";
import { v4 as uuidv4 } from "uuid";
import { addScrapeJob, waitForJob } from "../../services/queue-jobs";
import { logJob } from "../../services/logging/log_job";
import { search } from "../../search/v2";
import { isUrlBlocked } from "../../scraper/WebScraper/utils/blocklist";
import * as Sentry from "@sentry/node";
import { logger as _logger } from "../../lib/logger";
import type { Logger } from "winston";
import type { Engine } from "../../scraper/scrapeURL/engines";
import { getJobPriority } from "../../lib/job-priority";
import { CostTracking } from "../../lib/cost-tracking";
import { calculateCreditsToBeBilled } from "../../lib/scrape-billing";
import { supabase_service } from "../../services/supabase";
import { SearchV2Response } from "../../lib/entities";
import { ScrapeJobTimeoutError } from "../../lib/error";
import { scrapeQueue } from "../../services/worker/nuq";
import { z } from "zod";
import {
  buildSearchQuery,
  getCategoryFromUrl,
  CategoryOption,
} from "../../lib/search-query-builder";
// Removed heavy LLM/embedding imports in favor of SERP-anchored extract
import * as fs from "fs";
import * as path from "path";
import { generateText } from "ai";
import { getModel } from "../../lib/generic-ai";

// removed serpAnchoredExtract experiment

function writeSnippetResultLog(_payload: Record<string, any>) {
  // disabled logging
}

// ---------------- Agentic helpers (minimal) ----------------
async function expandQueriesPlanner(
  baseQuery: string,
  gapHint: string | undefined,
  maxVariants: number,
): Promise<string[]> {
  try {
    const prompt = `You improve a web search query and generate up to ${maxVariants} concise alternative queries (<= 12 words each). Focus on retrieving sources that directly answer the question. If hints of missing facts are provided, incorporate them.

Base query: ${baseQuery}
${gapHint ? `Missing facts to target: ${gapHint}` : ""}

Return ONLY a JSON array of strings.`;

    const { text } = await generateText({
      model: getModel("gpt-4o-mini", "openai"),
      prompt,
      temperature: 0.2,
    });

    let variants: string[];
    try {
      variants = JSON.parse(text);
      if (!Array.isArray(variants)) throw new Error("not array");
    } catch {
      let cleaned = text.trim();
      if (cleaned.startsWith("```json"))
        cleaned = cleaned.replace(/^```json/, "").trim();
      if (cleaned.startsWith("```"))
        cleaned = cleaned.replace(/^```/, "").trim();
      if (cleaned.endsWith("```")) cleaned = cleaned.replace(/```$/, "").trim();
      try {
        variants = JSON.parse(cleaned);
        if (!Array.isArray(variants)) throw new Error("not array");
      } catch {
        variants = cleaned
          .split(/\n|,/)
          .map(s => s.replace(/^[-*\d.\s]+/, "").trim())
          .filter(s => s.length > 0);
      }
    }

    const norm = (s: string) => s.replace(/\s+/g, " ").trim();
    const seen = new Set<string>();
    const out: string[] = [];
    for (const v of variants) {
      const q = norm(String(v));
      if (!q || seen.has(q.toLowerCase())) continue;
      seen.add(q.toLowerCase());
      out.push(q);
      if (out.length >= maxVariants) break;
    }
    const original = norm(baseQuery);
    return [
      original,
      ...out.filter(v => v.toLowerCase() !== original.toLowerCase()),
    ];
  } catch {
    return [baseQuery];
  }
}

function extractTopSpansFromMarkdown(
  markdown: string | undefined,
  query: string,
  maxSpans: number = 3,
): string[] {
  if (!markdown || markdown.trim().length === 0) return [];
  const normalize = (s: string) => s.replace(/\s+/g, " ").trim();
  const cleaned = markdown
    .replace(/\[!\[[\s\S]*?\]\([\s\S]*?\)\]\([\s\S]*?\)/g, "")
    .replace(/!\[[\s\S]*?\]\([\s\S]*?\)/g, "")
    .replace(/[\t ]+/g, " ")
    .replace(/\s*\n\s*/g, "\n");
  const blocks = cleaned
    .split(/\n\s*\n/)
    .map(p => normalize(p))
    .filter(p => p.length > 0 && !/^\s*[-*•]|^\s*\d+\./.test(p));

  const qTokens = Array.from(
    new Set(
      query
        .toLowerCase()
        .split(/[^a-z0-9]+/i)
        .filter(t => t.length >= 3),
    ),
  );
  const score = (p: string) => {
    const lt = p.toLowerCase();
    let s = 0;
    for (const t of qTokens) if (lt.includes(t)) s += 1;
    s += Math.min(p.length / 400, 1) * 0.25;
    return s;
  };
  const scored = blocks
    .map((p, i) => ({ p, i, s: score(p) }))
    .sort((a, b) => b.s - a.s)
    .slice(0, Math.max(1, maxSpans));

  return scored.map(({ p }) => (p.length > 550 ? p.slice(0, 547) + "..." : p));
}

async function evaluateAnswerWithLLM(
  query: string,
  spansByUrl: Array<{ url: string; spans: string[] }>,
): Promise<{ answered: boolean; confidence: number; missing_facts: string[] }> {
  try {
    const context = spansByUrl
      .map(
        ({ url, spans }, idx) =>
          `Source ${idx + 1}: ${url}\n${spans.map((s, i) => `Span ${i + 1}: ${s}`).join("\n")}`,
      )
      .join("\n\n");
    const prompt = `You are verifying if the question can be answered strictly from the provided spans. If yes, answer concisely and indicate high confidence; if not, list the missing facts needed.

Question: ${query}

Evidence (spans with sources):\n${context}

Return ONLY valid JSON with keys: answered (boolean), confidence (number 0..1), missing_facts (array of strings). Do not include any extra text.`;
    const { text } = await generateText({
      model: getModel("gpt-4o-mini", "openai"),
      prompt,
      temperature: 0,
    });
    try {
      const parsed = JSON.parse(text);
      return {
        answered: Boolean(parsed.answered),
        confidence: Number(parsed.confidence) || 0,
        missing_facts: Array.isArray(parsed.missing_facts)
          ? parsed.missing_facts.map(String)
          : [],
      };
    } catch {
      return { answered: false, confidence: 0, missing_facts: [] };
    }
  } catch {
    return { answered: false, confidence: 0, missing_facts: [] };
  }
}

// removed query expansion experiment

// ---- Retrieval fusion helpers ----
function textSim(a: string, b: string): number {
  const toTokens = (s: string) =>
    s
      .toLowerCase()
      .replace(/[^a-z0-9\s]+/g, " ")
      .split(/\s+/)
      .filter(t => t.length >= 3);
  const A = new Set(toTokens(a));
  const B = new Set(toTokens(b));
  if (A.size === 0 || B.size === 0) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter++;
  const uni = A.size + B.size - inter;
  return uni === 0 ? 0 : inter / uni; // Jaccard similarity
}

function rrfMerge<
  T extends {
    url?: string;
    position?: number;
    title?: string;
    description?: string;
  },
>(lists: (T[] | undefined)[], expandedQs: string[], k = 60, beta = 0.1): T[] {
  type Acc = {
    score: number;
    base: number;
    sim: number;
    bestItem: T;
    bestRank: number;
    bestVariantIdx: number;
  };
  const byUrl = new Map<string, Acc>();

  lists.forEach((list, variantIdx) => {
    list?.forEach((item, idx) => {
      const url = (item as any).url as string | undefined;
      if (!url) return;
      const rank = (item as any).position ?? idx + 1;
      const add = 1 / (k + rank);
      const prev = byUrl.get(url);
      if (!prev) {
        byUrl.set(url, {
          score: add,
          base: add,
          sim: 0,
          bestItem: item,
          bestRank: rank,
          bestVariantIdx: variantIdx,
        });
      } else {
        prev.score += add;
        prev.base += add;
        if (
          rank < prev.bestRank ||
          (rank === prev.bestRank && variantIdx < prev.bestVariantIdx)
        ) {
          prev.bestItem = item;
          prev.bestRank = rank;
          prev.bestVariantIdx = variantIdx;
        }
      }
    });
  });

  // add snippet-query similarity (snippet-only SSF)
  for (const acc of byUrl.values()) {
    const text =
      acc.bestItem.description && acc.bestItem.description.trim().length > 0
        ? (acc.bestItem.description as any)
        : `${(acc.bestItem as any).title ?? ""}`;
    let simSum = 0;
    for (const q of expandedQs) simSum += textSim(text, q);
    acc.sim = simSum;
    acc.score = acc.base + beta * simSum;
  }

  const ranked = Array.from(byUrl.values())
    .sort((a, b) => b.score - a.score)
    .map((entry, i) => ({ ...(entry.bestItem as any), position: i + 1 }));

  return ranked as any;
}

interface DocumentWithCostTracking {
  document: Document;
  costTracking: ReturnType<typeof CostTracking.prototype.toJSON>;
}

interface ScrapeJobInput {
  url: string;
  title: string;
  description: string;
}

async function startScrapeJob(
  searchResult: { url: string; title: string; description: string },
  options: {
    teamId: string;
    origin: string;
    timeout: number;
    scrapeOptions: ScrapeOptions;
    bypassBilling?: boolean;
    apiKeyId: number | null;
    forceEngine?: Engine | Engine[];
  },
  logger: Logger,
  flags: TeamFlags,
  directToBullMQ: boolean = false,
  isSearchPreview: boolean = false,
): Promise<string> {
  const jobId = uuidv4();

  const zeroDataRetention = flags?.forceZDR ?? false;

  logger.info("Adding scrape job", {
    scrapeId: jobId,
    url: searchResult.url,
    teamId: options.teamId,
    origin: options.origin,
    zeroDataRetention,
  });

  const jobPriority = await getJobPriority({
    team_id: options.teamId,
    basePriority: 10,
  });

  await addScrapeJob(
    {
      url: searchResult.url,
      mode: "single_urls",
      team_id: options.teamId,
      scrapeOptions: {
        ...options.scrapeOptions,
        // TODO: fix this
        maxAge: 3 * 24 * 60 * 60 * 1000, // 3 days
      },
      internalOptions: {
        teamId: options.teamId,
        bypassBilling: options.bypassBilling ?? true,
        zeroDataRetention,
        forceEngine: options.forceEngine,
      },
      origin: options.origin,
      // Do not touch this flag
      is_scrape: options.bypassBilling ?? false,
      startTime: Date.now(),
      zeroDataRetention,
      apiKeyId: options.apiKeyId,
    },
    jobId,
    jobPriority,
    directToBullMQ,
    true,
  );

  return jobId;
}

async function scrapeSearchResult(
  searchResult: { url: string; title: string; description: string },
  options: {
    teamId: string;
    origin: string;
    timeout: number;
    scrapeOptions: ScrapeOptions;
    bypassBilling?: boolean;
    apiKeyId: number | null;
    forceEngine?: Engine | Engine[];
  },
  logger: Logger,
  flags: TeamFlags,
  directToBullMQ: boolean = false,
  isSearchPreview: boolean = false,
): Promise<DocumentWithCostTracking> {
  try {
    // Start the scrape job
    const jobId = await startScrapeJob(
      searchResult,
      options,
      logger,
      flags,
      directToBullMQ,
      isSearchPreview,
    );

    const doc: Document = await waitForJob(jobId, options.timeout, false);

    logger.info("Scrape job completed", {
      scrapeId: jobId,
      url: searchResult.url,
      teamId: options.teamId,
      origin: options.origin,
    });

    await scrapeQueue.removeJob(jobId, logger);

    const document = {
      title: searchResult.title,
      description: searchResult.description,
      url: searchResult.url,
      ...doc,
    };

    let costTracking: ReturnType<typeof CostTracking.prototype.toJSON>;
    if (process.env.USE_DB_AUTHENTICATION === "true") {
      const { data: costTrackingResponse, error: costTrackingError } =
        await supabase_service
          .from("firecrawl_jobs")
          .select("cost_tracking")
          .eq("job_id", jobId);

      if (costTrackingError) {
        logger.error("Error getting cost tracking", {
          error: costTrackingError,
        });
        throw costTrackingError;
      }

      costTracking = costTrackingResponse?.[0]?.cost_tracking;
    } else {
      costTracking = new CostTracking().toJSON();
    }

    return {
      document,
      costTracking,
    };
  } catch (error) {
    const errMsg = error instanceof Error ? error.message : String(error);
    logger.error(`Error in scrapeSearchResult: ${errMsg}`, {
      url: searchResult.url,
      teamId: options.teamId,
    });

    const document: Document = {
      title: searchResult.title,
      description: searchResult.description,
      url: searchResult.url,
      metadata: {
        statusCode: 500,
        error: errMsg,
        proxyUsed: "basic",
      },
    };

    return {
      document,
      costTracking: new CostTracking().toJSON(),
    };
  }
}

export async function searchController(
  req: RequestWithAuth<{}, SearchResponse, SearchRequest>,
  res: Response<SearchResponse>,
) {
  // Get timing data from middleware (includes all middleware processing time)
  const middlewareStartTime =
    (req as any).requestTiming?.startTime || new Date().getTime();
  const controllerStartTime = new Date().getTime();

  const jobId = uuidv4();
  let logger = _logger.child({
    jobId,
    teamId: req.auth.team_id,
    module: "api/v2",
    method: "searchController",
    zeroDataRetention: req.acuc?.flags?.forceZDR,
  });

  if (req.acuc?.flags?.forceZDR) {
    return res.status(400).json({
      success: false,
      error:
        "Your team has zero data retention enabled. This is not supported on search. Please contact support@firecrawl.com to unblock this feature.",
    });
  }

  const middlewareTime = controllerStartTime - middlewareStartTime;
  const isSearchPreview =
    process.env.SEARCH_PREVIEW_TOKEN !== undefined &&
    process.env.SEARCH_PREVIEW_TOKEN === req.body.__searchPreviewToken;

  let credits_billed = 0;

  try {
    req.body = searchRequestSchema.parse(req.body);

    logger = logger.child({
      version: "v2",
      query: req.body.query,
      origin: req.body.origin,
    });

    let limit = req.body.limit;

    // Buffer results by 50% to account for filtered URLs
    const num_results_buffer = Math.floor(limit * 2);

    logger.info("Searching for results");

    // Extract unique types from sources for the search function
    // After transformation, sources is always an array of objects
    const searchTypes = [...new Set(req.body.sources.map((s: any) => s.type))];

    // Build category map once; expand queries into variants
    const { categoryMap } = buildSearchQuery(
      req.body.query,
      req.body.categories as CategoryOption[],
    );

    // 1) Query enrichment: get up to 5 variants (original first)
    // Agentic loop (minimal): iterate retrieval -> scrape -> extract -> evaluate -> optionally refine
    // Global scrape mode flags for this request scope
    const shouldScrape = true; // always enrich with TLS content
    const isAsyncScraping = false; // force sync to return enriched content in the same response
    const MAX_ITERATIONS = 2;
    let gapHint: string | undefined = undefined;
    let finalSearchResponse: SearchV2Response | null = null;
    let answered = false;
    let confidence = 0;

    for (let iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
      const expandedQueries = await expandQueriesPlanner(
        req.body.query,
        gapHint,
        5,
      );

      // 2) Run searches in parallel for each variant
      const variantSearches = await Promise.all(
        expandedQueries.map(
          q =>
            search({
              query: q,
              logger,
              advanced: false,
              num_results: num_results_buffer,
              tbs: req.body.tbs,
              filter: req.body.filter,
              lang: req.body.lang,
              country: req.body.country,
              location: req.body.location,
              type: searchTypes,
            }) as Promise<SearchV2Response>,
        ),
      );

      // 3) Reciprocal Rank Fusion (RRF) + snippet similarity across variants
      const searchResponse: SearchV2Response = {};

      searchResponse.web = rrfMerge(
        variantSearches.map(v => v.web),
        expandedQueries,
      );
      searchResponse.images = rrfMerge(
        variantSearches.map(v => v.images as any),
        expandedQueries,
      );
      searchResponse.news = rrfMerge(
        variantSearches.map(v => v.news as any),
        expandedQueries,
      );

      // 4) Add category labels to web results
      if (searchResponse.web && searchResponse.web.length > 0) {
        searchResponse.web = searchResponse.web.map(result => ({
          ...result,
          category: getCategoryFromUrl(result.url, categoryMap),
        }));
      }

      // Add category labels to news results
      if (searchResponse.news && searchResponse.news.length > 0) {
        searchResponse.news = searchResponse.news.map(result => ({
          ...result,
          category: result.url
            ? getCategoryFromUrl(result.url, categoryMap)
            : undefined,
        }));
      }

      // 5) Apply limit to each result type separately
      let totalResultsCount = 0;

      // Apply limit to web results
      if (searchResponse.web && searchResponse.web.length > 0) {
        if (searchResponse.web.length > limit) {
          searchResponse.web = searchResponse.web.slice(0, limit);
        }
        totalResultsCount += searchResponse.web.length;
      }

      // Apply limit to images
      if (searchResponse.images && searchResponse.images.length > 0) {
        if (searchResponse.images.length > limit) {
          searchResponse.images = searchResponse.images.slice(0, limit);
        }
        totalResultsCount += searchResponse.images.length;
      }

      // Apply limit to news
      if (searchResponse.news && searchResponse.news.length > 0) {
        if (searchResponse.news.length > limit) {
          searchResponse.news = searchResponse.news.slice(0, limit);
        }
        totalResultsCount += searchResponse.news.length;
      }

      // 6) TLS scrape only the already-selected (ranked & limited) results
      // use request-scope flags declared above

      if (!shouldScrape) {
        // No scraping - just count results for billing
        credits_billed = totalResultsCount;
      } else {
        // Common setup for both async and sync scraping
        logger.info(
          `Starting ${isAsyncScraping ? "async" : "sync"} search scraping`,
        );

        // Create common options
        const scrapeOptions = {
          teamId: req.auth.team_id,
          origin: req.body.origin,
          timeout: req.body.timeout,
          forceEngine: "fire-engine;tlsclient" as Engine,
          scrapeOptions: req.body.scrapeOptions,
          bypassBilling: !isAsyncScraping, // Async mode bills per job, sync mode bills manually
          apiKeyId: req.acuc?.api_key_id ?? null,
        };

        const directToBullMQ = true; // Send directly to BullMQ to avoid concurrency queue delays while waiting synchronously

        // Prepare all items to scrape with their original data
        const itemsToScrape: Array<{
          item: any;
          type: "web" | "news" | "image";
          scrapeInput: ScrapeJobInput;
        }> = [];

        // Add web results (skip blocked URLs)
        if (searchResponse.web) {
          searchResponse.web.forEach(item => {
            if (!isUrlBlocked(item.url, req.acuc?.flags ?? null)) {
              itemsToScrape.push({
                item,
                type: "web",
                scrapeInput: {
                  url: item.url,
                  title: item.title,
                  description: item.description,
                },
              });
            } else {
              logger.info(`Skipping blocked URL: ${item.url}`);
            }
          });
        }

        // Add news results (only those with URLs and not blocked)
        if (searchResponse.news) {
          searchResponse.news
            .filter(item => item.url)
            .forEach(item => {
              if (!isUrlBlocked(item.url!, req.acuc?.flags ?? null)) {
                itemsToScrape.push({
                  item,
                  type: "news",
                  scrapeInput: {
                    url: item.url!,
                    title: item.title || "",
                    description: item.snippet || "",
                  },
                });
              } else {
                logger.info(`Skipping blocked URL: ${item.url}`);
              }
            });
        }

        // Add image results (only those with URLs and not blocked)
        if (searchResponse.images) {
          searchResponse.images
            .filter(item => item.url)
            .forEach(item => {
              if (!isUrlBlocked(item.url!, req.acuc?.flags ?? null)) {
                itemsToScrape.push({
                  item,
                  type: "image",
                  scrapeInput: {
                    url: item.url!,
                    title: item.title || "",
                    description: "",
                  },
                });
              } else {
                logger.info(`Skipping blocked URL: ${item.url}`);
              }
            });
        }

        // Create all promises based on mode (async vs sync)
        const allPromises = itemsToScrape.map(({ scrapeInput }) =>
          isAsyncScraping
            ? startScrapeJob(
                scrapeInput,
                scrapeOptions,
                logger,
                req.acuc?.flags ?? null,
                directToBullMQ,
                isSearchPreview,
              )
            : scrapeSearchResult(
                scrapeInput,
                scrapeOptions,
                logger,
                req.acuc?.flags ?? null,
                directToBullMQ,
                isSearchPreview,
              ),
        );

        // Execute all operations in parallel
        const results = await Promise.all(allPromises);

        if (isAsyncScraping) {
          // Async mode: organize job IDs and return immediately
          const allJobIds = results as string[];
          const scrapeIds: {
            web?: string[];
            news?: string[];
            images?: string[];
          } = {};

          // Organize job IDs by type
          const webItems = itemsToScrape.filter(i => i.type === "web");
          const newsItems = itemsToScrape.filter(i => i.type === "news");
          const imageItems = itemsToScrape.filter(i => i.type === "image");

          let currentIndex = 0;

          if (webItems.length > 0) {
            scrapeIds.web = allJobIds.slice(
              currentIndex,
              currentIndex + webItems.length,
            );
            currentIndex += webItems.length;
          }

          if (newsItems.length > 0) {
            scrapeIds.news = allJobIds.slice(
              currentIndex,
              currentIndex + newsItems.length,
            );
            currentIndex += newsItems.length;
          }

          if (imageItems.length > 0) {
            scrapeIds.images = allJobIds.slice(
              currentIndex,
              currentIndex + imageItems.length,
            );
          }

          // Don't bill here - let each job bill itself when it completes
          credits_billed = allJobIds.length; // Just for reporting, not billing

          const endTime = new Date().getTime();
          const timeTakenInSeconds = (endTime - middlewareStartTime) / 1000;

          logger.info("Logging job (async scraping)", {
            num_docs: credits_billed,
            time_taken: timeTakenInSeconds,
            scrapeIds,
          });

          logJob(
            {
              job_id: jobId,
              success: true,
              num_docs:
                (searchResponse.web?.length ?? 0) +
                (searchResponse.images?.length ?? 0) +
                (searchResponse.news?.length ?? 0),
              docs: [searchResponse],
              time_taken: timeTakenInSeconds,
              team_id: req.auth.team_id,
              mode: "search",
              url: req.body.query,
              scrapeOptions: req.body.scrapeOptions,
              crawlerOptions: {
                ...req.body,
                query: undefined,
                scrapeOptions: undefined,
              },
              origin: req.body.origin,
              integration: req.body.integration,
              credits_billed,
              zeroDataRetention: false,
            },
            false,
            isSearchPreview,
          );

          // Log final timing information for async mode
          const totalRequestTime = new Date().getTime() - middlewareStartTime;
          const controllerTime = new Date().getTime() - controllerStartTime;
          logger.info("Search completed successfully (async)", {
            version: "v2",
            jobId,
            middlewareStartTime,
            controllerStartTime,
            middlewareTime,
            controllerTime,
            totalRequestTime,
            creditsUsed: credits_billed,
            scrapeful: shouldScrape,
          });

          return res.status(200).json({
            success: true,
            data: searchResponse,
            scrapeIds,
            creditsUsed: credits_billed,
          });
        } else {
          // Sync mode: process scraped documents
          const allDocsWithCostTracking = results as DocumentWithCostTracking[];
          const scrapedResponse: SearchV2Response = {};

          // Create a map of results indexed by URL for easy lookup
          const resultsMap = new Map<string, Document>();
          itemsToScrape.forEach((item, index) => {
            resultsMap.set(
              item.scrapeInput.url,
              allDocsWithCostTracking[index].document,
            );
          });

          // Process web results - preserve all original fields and add scraped content
          if (searchResponse.web && searchResponse.web.length > 0) {
            const useOnlySerpSnippet = true;
            scrapedResponse.web = await Promise.all(
              searchResponse.web.map(async (item, idx) => {
                if (useOnlySerpSnippet) {
                  return {
                    ...item,
                    ...(resultsMap.get(item.url) || {}),
                  } as any;
                }
                const doc = resultsMap.get(item.url);
                const enriched = { ...item, ...(doc || {}) } as any;
                return enriched;
              }),
            );
          }

          // Process news results - preserve all original fields and add scraped content
          if (searchResponse.news && searchResponse.news.length > 0) {
            scrapedResponse.news = searchResponse.news.map(item => {
              const doc = item.url ? resultsMap.get(item.url) : undefined;
              return {
                ...item, // Preserve ALL original fields
                ...doc, // Override/add scraped content
              };
            });
          }

          // Process image results - preserve all original fields and add scraped content
          if (searchResponse.images && searchResponse.images.length > 0) {
            scrapedResponse.images = searchResponse.images.map(item => {
              const doc = item.url ? resultsMap.get(item.url) : undefined;
              return {
                ...item, // Preserve ALL original fields
                ...doc, // Override/add scraped content
              };
            });
          }

          // Calculate credits
          const creditPromises = allDocsWithCostTracking.map(
            async docWithCost => {
              return await calculateCreditsToBeBilled(
                req.body.scrapeOptions,
                {
                  teamId: req.auth.team_id,
                  bypassBilling: true,
                  zeroDataRetention: false,
                },
                docWithCost.document,
                docWithCost.costTracking,
                req.acuc?.flags ?? null,
              );
            },
          );

          try {
            const individualCredits = await Promise.all(creditPromises);
            credits_billed = individualCredits.reduce(
              (sum, credit) => sum + credit,
              0,
            );
          } catch (error) {
            logger.error("Error calculating credits for billing", { error });
            credits_billed = totalResultsCount;
          }

          // Update response with scraped data
          Object.assign(searchResponse, scrapedResponse);

          // Minimal evaluation: build spans and ask LLM if answered
          const spansByUrl: Array<{ url: string; spans: string[] }> = [];
          if (searchResponse.web && searchResponse.web.length > 0) {
            for (const it of searchResponse.web as any[]) {
              if (
                it &&
                typeof it.markdown === "string" &&
                it.markdown.length > 0
              ) {
                const spans = extractTopSpansFromMarkdown(
                  it.markdown,
                  req.body.query,
                  3,
                );
                if (spans.length > 0) spansByUrl.push({ url: it.url, spans });
              }
            }
          }

          const evalResult = await evaluateAnswerWithLLM(
            req.body.query,
            spansByUrl,
          );
          answered = evalResult.answered && evalResult.confidence >= 0.6;
          confidence = evalResult.confidence || 0;
          gapHint = answered
            ? undefined
            : (evalResult.missing_facts || []).slice(0, 5).join("; ");
          logger.info("Agent iteration evaluation", {
            iteration,
            answered,
            confidence,
            hasGapHint: !!gapHint,
          });

          // Re-rank web results using snippet similarity only (keep TLS markdown in response)
          if (searchResponse.web && searchResponse.web.length > 0) {
            const simSum = (text: string | undefined, qs: string[]) => {
              if (!text || text.trim().length === 0) return 0;
              return qs.reduce((acc, q) => acc + textSim(text, q), 0);
            };

            const scored = searchResponse.web.map((item, idx) => {
              const snippetText =
                (item as any).description &&
                (item as any).description.trim().length > 0
                  ? (item as any).description
                  : `${(item as any).title ?? ""}`;
              const s1 = simSum(snippetText, expandedQueries);
              const score = s1;
              return { item, score, idx };
            });

            scored.sort((a, b) =>
              b.score !== a.score ? b.score - a.score : a.idx - b.idx,
            );
            searchResponse.web = scored.map((x, i) => ({
              ...(x.item as any),
              position: i + 1,
            }));
          }

          finalSearchResponse = searchResponse;
          if (answered) break;
        }
      }
    }

    const response = finalSearchResponse || {};

    // Bill team once for all successful results
    // - For sync scraping: Bill based on actual scraped content
    // - For async scraping: Jobs handle their own billing
    // - For no scraping: Bill based on search results count
    if (
      !isSearchPreview &&
      (!shouldScrape || (shouldScrape && !isAsyncScraping))
    ) {
      billTeam(
        req.auth.team_id,
        req.acuc?.sub_id ?? undefined,
        credits_billed,
        req.acuc?.api_key_id ?? null,
      ).catch(error => {
        logger.error(
          `Failed to bill team ${req.acuc?.sub_id} for ${credits_billed} credits: ${error}`,
        );
      });
    }

    const endTime = new Date().getTime();
    const timeTakenInSeconds = (endTime - middlewareStartTime) / 1000;

    logger.info("Logging job", {
      num_docs: credits_billed,
      time_taken: timeTakenInSeconds,
    });

    logJob(
      {
        job_id: jobId,
        success: true,
        num_docs:
          (response.web?.length ?? 0) +
          (response.images?.length ?? 0) +
          (response.news?.length ?? 0),
        docs: [response],
        time_taken: timeTakenInSeconds,
        team_id: req.auth.team_id,
        mode: "search",
        url: req.body.query,
        scrapeOptions: req.body.scrapeOptions,
        crawlerOptions: {
          ...req.body,
          query: undefined,
          scrapeOptions: undefined,
          asyncScraping: isAsyncScraping,
        },
        origin: req.body.origin,
        integration: req.body.integration,
        credits_billed,
        zeroDataRetention: false, // not supported
      },
      false,
      isSearchPreview,
    );

    // Log final timing information
    const totalRequestTime = new Date().getTime() - middlewareStartTime;
    const controllerTime = new Date().getTime() - controllerStartTime;

    logger.info("Request metrics", {
      version: "v2",
      jobId,
      mode: "search",
      middlewareStartTime,
      controllerStartTime,
      middlewareTime,
      controllerTime,
      totalRequestTime,
      creditsUsed: credits_billed,
      scrapeful: shouldScrape,
    });

    // For sync scraping or no scraping, don't include scrapeIds
    return res.status(200).json({
      success: true,
      data: response,
      creditsUsed: credits_billed,
    });
  } catch (error) {
    if (error instanceof z.ZodError) {
      logger.warn("Invalid request body", { error: error.errors });
      return res.status(400).json({
        success: false,
        error: "Invalid request body",
        details: error.errors,
      });
    }

    if (error instanceof ScrapeJobTimeoutError) {
      return res.status(408).json({
        success: false,
        code: error.code,
        error: error.message,
      });
    }

    Sentry.captureException(error);
    logger.error("Unhandled error occurred in search", {
      version: "v2",
      error,
    });
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

import { supabase_service } from "../supabase";
import "dotenv/config";
import { logger as _logger } from "../../lib/logger";
import { configDotenv } from "dotenv";
import {
  saveDeepResearchToGCS,
  saveExtractToGCS,
  saveLlmsTxtToGCS,
  saveMapToGCS,
  saveScrapeToGCS,
  saveSearchToGCS,
} from "../../lib/gcs-jobs";
import { hasFormatOfType } from "../../lib/format-utils";
import type { Document, ScrapeOptions } from "../../controllers/v2/types";
import type { CostTracking } from "../../lib/cost-tracking";
configDotenv();

type LoggedRequest = {
  id: string;
  kind:
    | "scrape"
    | "crawl"
    | "batch_scrape"
    | "search"
    | "extract"
    | "llmstxt"
    | "deep_research";
  api_version: string;
  team_id: string;
  origin: string;
  integration?: string | null;
  target_hint: string;
  dr_clean_by?: Date;
  zeroDataRetention: boolean;
};

export async function logRequest(request: LoggedRequest) {
  const { error } = await supabase_service.from("requests").insert({
    id: request.id,
    kind: request.kind,
    api_version: request.api_version,
    team_id:
      request.team_id === "preview" || request.team_id?.startsWith("preview_")
        ? null
        : request.team_id,
    origin: request.origin,
    integration: request.integration ?? null,
    target_hint: request.zeroDataRetention
      ? "<redacted due to zero data retention>"
      : request.target_hint,
    dr_clean_by: request.dr_clean_by ?? null,
  });
  if (error) {
    _logger.error("Error logging request", { error, requestId: request.id });
  }
}

export type LoggedScrape = {
  id: string;
  request_id: string;
  url: string;
  success: boolean;
  error?: string;
  doc?: Document;
  time_taken: number;
  team_id: string;
  options: ScrapeOptions;
  cost_tracking?: ReturnType<typeof CostTracking.prototype.toJSON>;
  pdf_num_pages?: number;
  credits_cost: number;
  zeroDataRetention: boolean;
};

export async function logScrape(scrape: LoggedScrape, force: boolean = false) {
  const { error } = await supabase_service.from("scrapes").insert({
    id: scrape.id,
    request_id: scrape.request_id,
    url: scrape.zeroDataRetention
      ? "<redacted due to zero data retention>"
      : scrape.url,
    success: scrape.success,
    error: scrape.error ?? null,
    time_taken: scrape.time_taken,
    team_id:
      scrape.team_id === "preview" || scrape.team_id?.startsWith("preview_")
        ? null
        : scrape.team_id,
    options: scrape.zeroDataRetention ? null : scrape.options,
    cost_tracking: scrape.zeroDataRetention
      ? null
      : (scrape.cost_tracking ?? null),
    pdf_num_pages: scrape.zeroDataRetention
      ? null
      : (scrape.pdf_num_pages ?? null),
    credits_cost: scrape.credits_cost,
  });
  if (error) {
    _logger.error("Error logging scrape", { error, scrapeId: scrape.id });
  }

  if (scrape.doc && process.env.GCS_BUCKET_NAME) {
    await saveScrapeToGCS(scrape);
  }

  if (scrape.success && !scrape.zeroDataRetention) {
    const hasMarkdown = hasFormatOfType(scrape.options.formats, "markdown");
    const hasChangeTracking = hasFormatOfType(
      scrape.options.formats,
      "changeTracking",
    );

    if (hasMarkdown || hasChangeTracking) {
      const { error } = await supabase_service.rpc(
        "change_tracking_insert_scrape",
        {
          p_team_id: scrape.team_id,
          p_url: scrape.url,
          p_job_id: scrape.id,
          p_change_tracking_tag: hasChangeTracking
            ? hasChangeTracking.tag
            : null,
          p_date_added: new Date().toISOString(),
        },
      );

      if (error) {
        _logger.warn("Error inserting into change_tracking_scrapes", {
          error,
          scrapeId: scrape.id,
          teamId: scrape.team_id,
        });
      } else {
        _logger.debug("Change tracking record inserted successfully");
      }
    }
  }
}

type LoggedCrawl = {
  id: string;
  request_id: string;
  url: string;
  team_id: string;
  options: any;
  num_docs: number;
  credits_cost: number;
  zeroDataRetention: boolean;
  cancelled: boolean;
};

export async function logCrawl(crawl: LoggedCrawl, force: boolean = false) {
  const { error } = await supabase_service.from("crawls").insert({
    id: crawl.id,
    request_id: crawl.request_id,
    url: crawl.zeroDataRetention
      ? "<redacted due to zero data retention>"
      : crawl.url,
    team_id:
      crawl.team_id === "preview" || crawl.team_id?.startsWith("preview_")
        ? null
        : crawl.team_id,
    options: crawl.zeroDataRetention ? null : crawl.options,
    num_docs: crawl.num_docs,
    credits_cost: crawl.credits_cost,
    cancelled: crawl.cancelled,
  });
  if (error) {
    _logger.error("Error logging crawl", { error, crawlId: crawl.id });
  }
}

type LoggedBatchScrape = {
  id: string;
  request_id: string;
  team_id: string;
  num_docs: number;
  credits_cost: number;
  zeroDataRetention: boolean;
  cancelled: boolean;
};

export async function logBatchScrape(
  batchScrape: LoggedBatchScrape,
  force: boolean = false,
) {
  const { error } = await supabase_service.from("batch_scrapes").insert({
    id: batchScrape.id,
    request_id: batchScrape.request_id,
    team_id:
      batchScrape.team_id === "preview" ||
      batchScrape.team_id?.startsWith("preview_")
        ? null
        : batchScrape.team_id,
    num_docs: batchScrape.num_docs,
    credits_cost: batchScrape.credits_cost,
    cancelled: batchScrape.cancelled,
  });
  if (error) {
    _logger.error("Error logging batch scrape", {
      error,
      batchScrapeId: batchScrape.id,
    });
  }
}

export type LoggedSearch = {
  id: string;
  request_id: string;
  query: string;
  team_id: string;
  options: any;
  time_taken: number;
  credits_cost: number;
  success: boolean;
  error?: string;
  num_results: number;
  results: any;
  zeroDataRetention: boolean;
};

export async function logSearch(search: LoggedSearch, force: boolean = false) {
  const { error } = await supabase_service.from("searches").insert({
    id: search.id,
    request_id: search.request_id,
    query: search.zeroDataRetention
      ? "<redacted due to zero data retention>"
      : search.query,
    team_id:
      search.team_id === "preview" || search.team_id?.startsWith("preview_")
        ? null
        : search.team_id,
    options: search.zeroDataRetention ? null : search.options,
    credits_cost: search.credits_cost,
    success: search.success,
    error: search.zeroDataRetention ? null : (search.error ?? null),
    num_results: search.num_results,
  });

  if (search.results && !search.zeroDataRetention) {
    await saveSearchToGCS(search);
  }

  if (error) {
    _logger.error("Error logging search", { error, searchId: search.id });
  }
}

export type LoggedExtract = {
  id: string;
  request_id: string;
  urls: string[];
  team_id: string;
  options: any;
  model_kind: "fire-0" | "fire-1";
  credits_cost: number;
  success: boolean;
  error?: string;
  result?: any;
  cost_tracking?: ReturnType<typeof CostTracking.prototype.toJSON>;
};

export async function logExtract(
  extract: LoggedExtract,
  force: boolean = false,
) {
  const { error } = await supabase_service.from("extracts").insert({
    id: extract.id,
    request_id: extract.request_id,
    urls: extract.urls,
    team_id:
      extract.team_id === "preview" || extract.team_id?.startsWith("preview_")
        ? null
        : extract.team_id,
    options: extract.options,
    model_kind: extract.model_kind,
    credits_cost: extract.credits_cost,
    success: extract.success,
    error: extract.error ?? null,
    cost_tracking: extract.cost_tracking ?? null,
  });

  if (extract.result) {
    await saveExtractToGCS(extract);
  }

  if (error) {
    _logger.error("Error logging extract", { error, extractId: extract.id });
  }
}

export type LoggedMap = {
  id: string;
  request_id: string;
  url: string;
  team_id: string;
  options: any;
  results: any[];
  credits_cost: number;
  zeroDataRetention: boolean;
};

export async function logMap(map: LoggedMap, force: boolean = false) {
  const { error } = await supabase_service.from("maps").insert({
    id: map.id,
    request_id: map.request_id,
    url: map.zeroDataRetention
      ? "<redacted due to zero data retention>"
      : map.url,
    team_id:
      map.team_id === "preview" || map.team_id?.startsWith("preview_")
        ? null
        : map.team_id,
    options: map.zeroDataRetention ? null : map.options,
    num_results: map.results.length,
    credits_cost: map.credits_cost,
  });

  if (map.results && !map.zeroDataRetention) {
    await saveMapToGCS(map);
  }

  if (error) {
    _logger.error("Error logging map", { error, mapId: map.id });
  }
}

export type LoggedLlmsTxt = {
  id: string;
  request_id: string;
  url: string;
  team_id: string;
  options: any;
  num_urls: number;
  cost_tracking?: ReturnType<typeof CostTracking.prototype.toJSON>;
  credits_cost: number;
  result: { llmstxt: string; llmsfulltxt: string };
};

export async function logLlmsTxt(
  llmsTxt: LoggedLlmsTxt,
  force: boolean = false,
) {
  const { error } = await supabase_service.from("llmstxts").insert({
    id: llmsTxt.id,
    request_id: llmsTxt.request_id,
    url: llmsTxt.url,
    team_id:
      llmsTxt.team_id === "preview" || llmsTxt.team_id?.startsWith("preview_")
        ? null
        : llmsTxt.team_id,
    options: llmsTxt.options,
    num_urls: llmsTxt.num_urls,
    credits_cost: llmsTxt.credits_cost,
    cost_tracking: llmsTxt.cost_tracking ?? null,
  });

  if (llmsTxt.result) {
    await saveLlmsTxtToGCS(llmsTxt);
  }

  if (error) {
    _logger.error("Error logging llmstxt", { error, llmsTxtId: llmsTxt.id });
  }
}

export type LoggedDeepResearch = {
  id: string;
  request_id: string;
  query: string;
  team_id: string;
  options: any;
  time_taken: number;
  credits_cost: number;
  result: { finalAnalysis: string; sources: any; json: any };
  cost_tracking?: ReturnType<typeof CostTracking.prototype.toJSON>;
};

export async function logDeepResearch(
  deepResearch: LoggedDeepResearch,
  force: boolean = false,
) {
  const { error } = await supabase_service.from("deep_researches").insert({
    id: deepResearch.id,
    request_id: deepResearch.request_id,
    query: deepResearch.query,
    team_id:
      deepResearch.team_id === "preview" ||
      deepResearch.team_id?.startsWith("preview_")
        ? null
        : deepResearch.team_id,
    options: deepResearch.options,
    time_taken: deepResearch.time_taken,
    credits_cost: deepResearch.credits_cost,
    cost_tracking: deepResearch.cost_tracking ?? null,
  });

  if (deepResearch.result) {
    await saveDeepResearchToGCS(deepResearch);
  }

  if (error) {
    _logger.error("Error logging deep research", {
      error,
      deepResearchId: deepResearch.id,
    });
  }
}

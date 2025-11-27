import { supabase_service } from "../../../services/supabase";
import { getJobFromGCS } from "../../../lib/gcs-jobs";
import {
  scrape,
  crawl,
  batchScrape,
  scrapeStatusRaw,
  zdrcleaner,
  idmux,
} from "./lib";
import { readFile, stat } from "node:fs/promises";
import { describeIf, TEST_PRODUCTION } from "../lib";

const logIgnoreList = [
  "Billing queue created",
  "No billing operations to process in batch",
  "billing batch queue",
  "billing batch processing lock",
  "Batch billing team",
  "Successfully billed team",
  "Billing batch processing",
  "Processing batch of",
  "Billing team",
  "No jobs to process",
  "nuqHealthCheck metrics",
  "nuqGetJobToProcess metrics",
  "Domain frequency processor",
  "billing operation to batch queue",
  "billing operation to queue",
  "billing operation for team",
  "Added billing operation to queue",
  "Index RF inserter found",
  "Redis connected",
  "Prefetched jobs",
  "nuqPrefetchJobs metrics",
  "request completed",
  "nuqAddJobs metrics",
  "nuqGetJobs metrics",
  "nuqAddGroup metrics",
  "nuqGetGroup metrics",
  "NuQ job prefetch sent",
  "Acquired job",
  "nuqGetJob metrics",
  "nuqJobFinish metrics",
  "Starting to update tallies",
  "tally for team",
];

async function getLogs() {
  let logs: string;
  try {
    await stat("firecrawl.log");
  } catch (e) {
    console.warn("No firecrawl.log file found");
    return [];
  }
  logs = await readFile("firecrawl.log", "utf8");
  return logs
    .split("\n")
    .filter(
      x => x.trim().length > 0 && !logIgnoreList.some(y => x.includes(y)),
    );
}

describeIf(TEST_PRODUCTION)("Zero Data Retention", () => {
  describe.each(["Team-scoped", "Request-scoped"] as const)("%s", scope => {
    it("should clean up a scrape immediately", async () => {
      let identity = await idmux({
        name: `zdr/${scope}/scrape`,
        credits: 10000,
        flags: {
          allowZDR: true,
          ...(scope === "Team-scoped"
            ? {
                forceZDR: true,
              }
            : {}),
        },
      });

      const testId = crypto.randomUUID();
      const scrape1 = await scrape(
        {
          url: "https://firecrawl.dev/?test=" + testId,
          zeroDataRetention: scope === "Request-scoped" ? true : undefined,
        },
        identity,
      );

      const gcsJob = await getJobFromGCS(scrape1.metadata.scrapeId!);
      expect(gcsJob).toBeNull();

      // Check the scrapes table for the scrape record
      const { data: scrapeData, error: scrapeError } = await supabase_service
        .from("scrapes")
        .select("*")
        .eq("id", scrape1.metadata.scrapeId!)
        .limit(1);

      expect(scrapeError).toBeFalsy();
      expect(scrapeData).toHaveLength(1);

      if (scrapeData && scrapeData.length === 1) {
        const record = scrapeData[0];
        expect(record.url).not.toContain("://"); // no url stored
        expect(record.options).toBeNull();
      }

      if (scope === "Request-scoped") {
        const status = await scrapeStatusRaw(
          scrape1.metadata.scrapeId!,
          identity,
        );

        expect(status.statusCode).toBe(404);
      }
    }, 60000);

    it("should clean up a crawl", async () => {
      const preLogs = await getLogs();

      let identity = await idmux({
        name: `zdr/${scope}/crawl`,
        credits: 10000,
        flags: {
          allowZDR: true,
          ...(scope === "Team-scoped"
            ? {
                forceZDR: true,
              }
            : {}),
        },
      });

      const crawl1 = await crawl(
        {
          url: "https://firecrawl.dev",
          limit: 10,
          zeroDataRetention: scope === "Request-scoped" ? true : undefined,
        },
        identity,
      );

      const postLogs = (await getLogs()).slice(preLogs.length);

      if (postLogs.length > 0) {
        console.warn("Logs changed during crawl", postLogs);
      }

      expect(postLogs).toHaveLength(0);

      // Check the crawls table for the crawl record
      const { data: crawlData, error: crawlError } = await supabase_service
        .from("crawls")
        .select("*")
        .eq("id", crawl1.id)
        .limit(1);

      expect(crawlError).toBeFalsy();
      expect(crawlData).toHaveLength(1);

      if (crawlData && crawlData.length === 1) {
        const record = crawlData[0];
        expect(record.url).not.toContain("://"); // no url stored
        expect(record.options).toBeNull();
      }

      // Check the scrapes table for individual scrapes in this crawl
      const { data: scrapes, error: scrapesError } = await supabase_service
        .from("scrapes")
        .select("*")
        .eq("request_id", crawl1.id);

      expect(scrapesError).toBeFalsy();
      expect((scrapes ?? []).length).toBeGreaterThanOrEqual(1);

      for (const scrapeRecord of scrapes ?? []) {
        expect(scrapeRecord.url).not.toContain("://"); // no url stored
        expect(scrapeRecord.options).toBeNull();

        if (scrapeRecord.success) {
          const gcsJob = await getJobFromGCS(scrapeRecord.id);
          expect(gcsJob).not.toBeNull(); // clean up happens async on a worker after expiry
        }
      }

      await zdrcleaner(identity.teamId!);

      for (const scrapeRecord of scrapes ?? []) {
        const gcsJob = await getJobFromGCS(scrapeRecord.id);
        expect(gcsJob).toBeNull();

        if (scope === "Request-scoped") {
          const status = await scrapeStatusRaw(scrapeRecord.id, identity);
          expect(status.statusCode).toBe(404);
        }
      }
    }, 600000);

    it("should clean up a batch scrape", async () => {
      const preLogs = await getLogs();

      let identity = await idmux({
        name: `zdr/${scope}/batch-scrape`,
        credits: 10000,
        flags: {
          allowZDR: true,
          ...(scope === "Team-scoped"
            ? {
                forceZDR: true,
              }
            : {}),
        },
      });

      const crawl1 = await batchScrape(
        {
          urls: ["https://firecrawl.dev", "https://mendable.ai"],
          zeroDataRetention: scope === "Request-scoped" ? true : undefined,
        },
        identity,
      );

      const postLogs = (await getLogs()).slice(preLogs.length);

      if (postLogs.length > 0) {
        console.warn("Logs changed during batch scrape", postLogs);
      }

      expect(postLogs).toHaveLength(0);

      // Check the batch_scrapes table for the batch scrape record
      const { data: batchData, error: batchError } = await supabase_service
        .from("batch_scrapes")
        .select("*")
        .eq("id", crawl1.id)
        .limit(1);

      expect(batchError).toBeFalsy();
      expect(batchData).toHaveLength(1);

      // Check the scrapes table for individual scrapes in this batch
      const { data: scrapes, error: scrapesError } = await supabase_service
        .from("scrapes")
        .select("*")
        .eq("request_id", crawl1.id);

      expect(scrapesError).toBeFalsy();
      expect((scrapes ?? []).length).toBe(2);

      for (const scrapeRecord of scrapes ?? []) {
        expect(scrapeRecord.url).not.toContain("://"); // no url stored
        expect(scrapeRecord.options).toBeNull();

        if (scrapeRecord.success) {
          const gcsJob = await getJobFromGCS(scrapeRecord.id);
          expect(gcsJob).not.toBeNull(); // clean up happens async on a worker after expiry
        }
      }

      await zdrcleaner(identity.teamId!);

      for (const scrapeRecord of scrapes ?? []) {
        const gcsJob = await getJobFromGCS(scrapeRecord.id);
        expect(gcsJob).toBeNull();

        if (scope === "Request-scoped") {
          const status = await scrapeStatusRaw(scrapeRecord.id, identity);
          expect(status.statusCode).toBe(404);
        }
      }
    }, 600000);
  });
});

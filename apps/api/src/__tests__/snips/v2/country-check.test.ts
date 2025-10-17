import { Identity, idmux, scrapeTimeout, scrapeRaw } from "./lib";

describe("V2 Country Check Skip Flag", () => {
  let identityWithFlag: Identity;
  let identityWithoutFlag: Identity;

  beforeAll(async () => {
    identityWithFlag = await idmux({
      name: "v2-country-check-skip",
      concurrency: 100,
      credits: 1000000,
      flags: { skipCountryCheck: true },
    });

    identityWithoutFlag = await idmux({
      name: "v2-country-check-no-skip",
      concurrency: 100,
      credits: 1000000,
    });
  }, 10000);

  test(
    "should skip country check when skipCountryCheck flag is true",
    async () => {
      const response = await scrapeRaw(
        {
          url: "https://firecrawl.dev",
          actions: [
            {
              type: "wait",
              milliseconds: 100,
            },
          ],
        },
        identityWithFlag,
      );

      expect(response.status).toBe(200);
      expect(response.body.success).toBe(true);
    },
    scrapeTimeout,
  );

  test(
    "should allow headers when skipCountryCheck flag is true",
    async () => {
      const response = await scrapeRaw(
        {
          url: "https://firecrawl.dev",
          headers: {
            "User-Agent": "Custom User Agent",
          },
        },
        identityWithFlag,
      );

      expect(response.status).toBe(200);
      expect(response.body.success).toBe(true);
    },
    scrapeTimeout,
  );
});

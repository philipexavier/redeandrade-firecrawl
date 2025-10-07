import "dotenv/config";
import { z } from "zod";
import { logger } from "./lib/logger";

// need these optional wrappers in-case of empty strings in env files
const optionalUrl = z.preprocess(
  (v: unknown) =>
    typeof v === "string" && v.trim().length > 0 ? v : undefined,
  z.string().url().optional(),
);

const defaultUrl = (defaultValue: string, message?: string) =>
  z.preprocess(
    (v: unknown) =>
      typeof v === "string" && v.trim().length > 0 ? v : defaultValue,
    z.string().url(message),
  );

const optionalStr = z.preprocess(
  (v: unknown) =>
    typeof v === "string" && v.trim().length > 0 ? v : undefined,
  z.string().optional(),
);

const defaultString = (defaultValue: string) =>
  z.preprocess(
    (v: unknown) =>
      typeof v === "string" && v.trim().length > 0 ? v : defaultValue,
    z.string(),
  );

const strbool = (defaultValue: boolean, message?: string) =>
  z.preprocess(
    val => (val === "true" || val === "1" ? true : val),
    z
      .boolean({
        message,
      })
      .default(defaultValue),
  );

const port = (def: number) =>
  z
    .preprocess(
      v => (typeof v === "string" && v.trim().length > 0 ? v : def),
      z.coerce.number().int().min(1).max(65535),
    )
    .default(def);

// Only using Zod for the env vars that are used for self-hosting or are inside .env.example for now
// Designed to help users avoid common mistakes when setting up self-hosted instances
const base = z.object({
  HOST: z.string().default("0.0.0.0"),

  PORT: port(3002),
  EXTRACT_WORKER_PORT: port(3004),
  WORKER_PORT: port(3005),

  LOGGING_LEVEL: z
    .enum(["NONE", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"])
    .default("INFO"),

  NUQ_DATABASE_URL: defaultUrl(
    "postgres://postgres:postgres@nuq-postgres:5432/postgres",
    "Must be a valid database URL",
  ),

  REDIS_URL: defaultUrl("redis://redis:6379", "Must be a valid Redis URL"),
  REDIS_RATE_LIMIT_URL: defaultUrl(
    "redis://redis:6379",
    "Must be a valid Redis URL",
  ),
  PLAYWRIGHT_MICROSERVICE_URL: optionalUrl,

  BULL_AUTH_KEY: z
    .string()
    .min(1)
    .regex(
      /^[@A-Za-z0-9_-]+$/,
      "Only alphanumeric characters, underscores, hyphens, and @ are allowed",
    )
    .default("CHANGEME"),

  // TODO: should probably change everything to use this as it enforces the default false
  USE_DB_AUTHENTICATION: strbool(false, "Must be true or false"),

  SELF_HOSTED_WEBHOOK_URL: optionalUrl,
  SELF_HOSTED_WEBHOOK_HMAC_SECRET: optionalStr,
  ALLOW_LOCAL_WEBHOOKS: strbool(false, "Must be true or false"),

  PROXY_SERVER: optionalStr,
  PROXY_USERNAME: optionalStr,
  PROXY_PASSWORD: optionalStr,

  SEARCHAPI_API_KEY: optionalStr,
  SEARCHAPI_ENGINE: defaultString("google"),

  SERPER_API_KEY: optionalStr,

  SEARXNG_ENDPOINT: optionalUrl,
  SEARXNG_ENGINES: defaultString(""),
  SEARXNG_CATEGORIES: defaultString(""),

  RESEND_API_KEY: optionalStr,
  SLACK_WEBHOOK_URL: optionalUrl,

  POSTHOG_API_KEY: optionalStr,
  POSTHOG_HOST: optionalStr,

  X402_PAY_TO_ADDRESS: optionalStr,
  X402_NETWORK: z
    .enum(["base-sepolia", "base", "avalanche-fuji", "avalanche", "iotex"])
    .default("base-sepolia"),
  X402_FACILITATOR_URL: defaultUrl("https://x402.org/facilitator"),
  X402_ENDPOINT_PRICE_USD: defaultString("0.01"),

  MAX_CPU: z.coerce.number().default(0.8),
  MAX_RAM: z.coerce.number().default(0.8),
  SYS_INFO_MAX_CACHE_DURATION: z.coerce.number().default(150),
});

const llmProviders = z.object({
  OPENAI_API_KEY: optionalStr,
  OPENAI_BASE_URL: optionalUrl,

  OLLAMA_BASE_URL: optionalUrl,

  ANTHROPIC_API_KEY: optionalStr,
  GROQ_API_KEY: optionalStr,
  OPENROUTER_API_KEY: optionalStr,
  FIREWORKS_API_KEY: optionalStr,
  DEEPINFRA_API_KEY: optionalStr,

  GOOGLE_GENERATIVE_AI_API_KEY: optionalStr,
  VERTEX_CREDENTIALS: optionalStr,

  // Note: MODEL_NAME works in a strange way
  // it overrides the model used for all providers, regardless of their supported models, maybe this should be changed in future
  MODEL_NAME: optionalStr,
  MODEL_EMBEDDING_NAME: optionalStr,
});

const internal = z.object({
  JOB_LOCK_EXTEND_INTERVAL: z.coerce.number().min(1).default(10000), // 10 seconds
  JOB_LOCK_EXTENSION_TIME: z.coerce.number().min(1).default(60000), // 60 seconds
  CANT_ACCEPT_CONNECTION_INTERVAL: z.coerce.number().min(1).default(2000), // 2 seconds
  CONNECTION_MONITOR_INTERVAL: z.coerce.number().min(1).default(10), // 10 ms
  GOT_JOB_INTERVAL: z.coerce.number().min(1).default(20), // 20 ms
});

const envSchema = base.merge(internal).merge(llmProviders);

let config: z.infer<typeof envSchema>;

try {
  config = envSchema.parse(process.env);
} catch (err) {
  if (err instanceof z.ZodError) {
    logger.error("Problems found with environment variables:");
    for (const issue of err.issues) {
      if (issue.path && issue.path.length > 0) {
        logger.error(` - ${issue.path.join(".")}: ${issue.message}`);
      } else {
        logger.error(` - ${issue.message}`);
      }
    }
    logger.error("");
  } else {
    logger.error("Unexpected error:", err);
  }
  process.exit(1);
}

export { config };

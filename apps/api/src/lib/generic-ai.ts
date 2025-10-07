import { config } from "../config";
import { createOpenAI } from "@ai-sdk/openai";
import { createOllama } from "ollama-ai-provider";
import { anthropic } from "@ai-sdk/anthropic";
import { groq } from "@ai-sdk/groq";
import { google } from "@ai-sdk/google";
import { createOpenRouter } from "@openrouter/ai-sdk-provider";
import { fireworks } from "@ai-sdk/fireworks";
import { deepinfra } from "@ai-sdk/deepinfra";
import { createVertex } from "@ai-sdk/google-vertex";
import { existsSync } from "fs";
import { logger } from "./logger";

type Provider =
  | "openai"
  | "ollama"
  | "anthropic"
  | "groq"
  | "google"
  | "openrouter"
  | "fireworks"
  | "deepinfra"
  | "vertex";

const defaultProvider: Provider = process.env.OLLAMA_BASE_URL
  ? "ollama"
  : "openai";

const createGoogleVertexAuthOptions = () => {
  const vertexCredentials = config.VERTEX_CREDENTIALS;
  if (vertexCredentials) {
    try {
      return {
        credentials: JSON.parse(atob(vertexCredentials)),
      };
    } catch (e) {
      const fileExists = existsSync(vertexCredentials);
      if (fileExists) {
        return {
          keyFile: vertexCredentials,
        };
      } else {
        logger.error(
          "Failed to parse VERTEX_CREDENTIALS environment variable. It should be a base64-encoded JSON string or a valid file path. Failing back to default keyFile.",
        );
      }
    }
  }

  // note: if this does not exist, Vertex API calls will fail
  return {
    keyFile: "./gke-key.json",
  };
};

// TODO: probably want to improve the provider system, seems to be highly used and is not very flexible for self-hosted users currently (hard coded providers etc.)
const providerList: Record<Provider, any> = {
  // OPENAI_API_KEY
  openai: createOpenAI({
    apiKey: config.OPENAI_API_KEY,
    baseURL: config.OPENAI_BASE_URL,
  }),
  ollama: createOllama({
    baseURL: config.OLLAMA_BASE_URL,
  }),
  openrouter: createOpenRouter({
    apiKey: config.OPENROUTER_API_KEY,
  }),
  anthropic, // ANTHROPIC_API_KEY
  groq, // GROQ_API_KEY
  google, // GOOGLE_GENERATIVE_AI_API_KEY
  fireworks, // FIREWORKS_API_KEY
  deepinfra, // DEEPINFRA_API_KEY
  vertex: createVertex({
    project: "firecrawl",
    //https://github.com/vercel/ai/issues/6644 bug -- appears to be resolved
    baseURL:
      "https://aiplatform.googleapis.com/v1/projects/firecrawl/locations/global/publishers/google",
    location: "global",
    googleAuthOptions: createGoogleVertexAuthOptions(),
  }),
};

export function getModel(name: string, provider: Provider = defaultProvider) {
  if (name === "gemini-2.5-pro") {
    name = "gemini-2.5-pro";
  }

  return process.env.MODEL_NAME
    ? providerList[provider](process.env.MODEL_NAME)
    : providerList[provider](name);
}

export function getEmbeddingModel(
  name: string,
  provider: Provider = defaultProvider,
) {
  return process.env.MODEL_EMBEDDING_NAME
    ? providerList[provider].embedding(process.env.MODEL_EMBEDDING_NAME)
    : providerList[provider].embedding(name);
}

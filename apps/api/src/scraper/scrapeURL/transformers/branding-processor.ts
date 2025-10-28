/**
 * Node.js branding processor
 * Takes raw data from browser and processes it here for better performance
 */

import { BrandingProfile } from "../../../types/branding";

interface RawBrandingData {
  cssData: {
    colors: string[];
    spacings: number[];
    radii: number[];
  };
  snapshots: Array<{
    tag: string;
    classes: string;
    text: string;
    rect: { w: number; h: number };
    colors: {
      text: string;
      background: string;
      border: string;
      borderWidth: number | null;
    };
    typography: {
      fontStack: string[];
      size: string | null;
      weight: number | null;
    };
    radius: number | null;
    isButton: boolean;
    isInput: boolean;
    isLink: boolean;
  }>;
  images: Array<{ type: string; src: string }>;
  typography: {
    stacks: {
      body: string[];
      heading: string[];
      paragraph: string[];
    };
    sizes: {
      h1: string;
      h2: string;
      body: string;
    };
  };
  frameworkHints: string[];
  colorScheme: "light" | "dark";
}

// Convert rgba/rgb to hex
function hexify(rgba: string): string | null {
  if (!rgba) return null;

  // Already hex
  if (/^#([0-9a-f]{3,8})$/i.test(rgba)) {
    if (rgba.length === 4) {
      return (
        "#" +
        [...rgba.slice(1)]
          .map(ch => ch + ch)
          .join("")
          .toUpperCase()
      );
    }
    if (rgba.length === 7) return rgba.toUpperCase();
    if (rgba.length === 9) return rgba.slice(0, 7).toUpperCase();
    return rgba.toUpperCase();
  }

  // Parse Display P3
  const colorMatch = rgba.match(
    /color\((?:display-p3|srgb)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)/i,
  );
  if (colorMatch) {
    const [r, g, b] = colorMatch
      .slice(1, 4)
      .map(n => Math.max(0, Math.min(255, Math.round(parseFloat(n) * 255))));
    return (
      "#" +
      [r, g, b]
        .map(x => x.toString(16).padStart(2, "0"))
        .join("")
        .toUpperCase()
    );
  }

  // Parse rgb/rgba
  const match = rgba.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
  if (!match) return null;
  const [r, g, b] = match
    .slice(1, 4)
    .map(n => Math.max(0, Math.min(255, parseInt(n, 10))));
  return (
    "#" +
    [r, g, b]
      .map(x => x.toString(16).padStart(2, "0"))
      .join("")
      .toUpperCase()
  );
}

// Calculate contrast for text readability
function contrastYIQ(hex: string): number {
  if (!hex) return 0;
  const h = hex.replace("#", "");
  if (h.length < 6) return 0;
  const r = parseInt(h.slice(0, 2), 16),
    g = parseInt(h.slice(2, 4), 16),
    b = parseInt(h.slice(4, 6), 16);
  return (r * 299 + g * 587 + b * 114) / 1000;
}

// Check if color is valid (not pure black/white/transparent)
function isColorValid(color: string | null): boolean {
  if (!color) return false;
  if (color.includes("transparent")) return false;
  if (/^#(FFF(FFF)?|000(000)?|F{6}|0{6})$/i.test(color)) return false;
  const yiq = contrastYIQ(color);
  return yiq < 240;
}

// Infer color palette from snapshots
function inferPalette(
  snapshots: RawBrandingData["snapshots"],
  cssColors: string[],
) {
  const freq = new Map<string, number>();
  const bump = (hex: string | null, weight = 1) => {
    if (!hex) return;
    freq.set(hex, (freq.get(hex) || 0) + weight);
  };

  for (const s of snapshots) {
    const area = Math.max(1, s.rect.w * s.rect.h);
    bump(hexify(s.colors.background), 0.5 + Math.log10(area + 10));
    bump(hexify(s.colors.text), 1.0);
    bump(hexify(s.colors.border), 0.3);
  }

  for (const c of cssColors) bump(hexify(c), 0.5);

  const ranked = Array.from(freq.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([h]) => h);

  const isGrayish = (hex: string) => {
    const h = hex.replace("#", "");
    if (h.length < 6) return true;
    const r = parseInt(h.slice(0, 2), 16),
      g = parseInt(h.slice(2, 4), 16),
      b = parseInt(h.slice(4, 6), 16);
    const max = Math.max(r, g, b),
      min = Math.min(r, g, b);
    return max - min < 15;
  };

  const background =
    ranked.find(h => isGrayish(h) && contrastYIQ(h) > 180) || "#FFFFFF";
  const textPrimary =
    ranked.find(h => !/^#FFFFFF$/i.test(h) && contrastYIQ(h) < 160) ||
    "#111111";
  const primary =
    ranked.find(h => !isGrayish(h) && h !== textPrimary && h !== background) ||
    "#000000";
  const accent = ranked.find(h => h !== primary && !isGrayish(h)) || primary;

  return {
    primary,
    accent,
    background,
    text_primary: textPrimary,
    link: accent,
  };
}

// Infer spacing base unit
function inferBaseUnit(values: number[]): number {
  const vs = values
    .filter(v => Number.isFinite(v) && v > 0 && v <= 128)
    .map(v => Math.round(v));
  if (vs.length === 0) return 8;
  const candidates = [4, 6, 8, 10, 12];
  for (const c of candidates) {
    const ok =
      vs.filter(v => v % c === 0 || Math.abs((v % c) - c) <= 1 || v % c <= 1)
        .length / vs.length;
    if (ok >= 0.6) return c;
  }
  vs.sort((a, b) => a - b);
  const med = vs[Math.floor(vs.length / 2)];
  return Math.max(2, Math.min(12, Math.round(med / 2) * 2));
}

// Pick common border radius
function pickBorderRadius(radii: (number | null)[]): string {
  const rs = radii.filter((v): v is number => Number.isFinite(v));
  if (!rs.length) return "8px";
  rs.sort((a, b) => a - b);
  const med = rs[Math.floor(rs.length / 2)];
  return Math.round(med) + "px";
}

// Infer fonts list from stacks
function inferFontsList(
  fontStacks: string[][],
): Array<{ family: string; count: number }> {
  const freq: Record<string, number> = {};
  for (const stack of fontStacks) {
    for (const f of stack) {
      if (f) freq[f] = (freq[f] || 0) + 1;
    }
  }

  return Object.keys(freq)
    .sort((a, b) => freq[b] - freq[a])
    .slice(0, 10)
    .map(f => ({ family: f, count: freq[f] }));
}

// Pick logo from images
function pickLogo(images: Array<{ type: string; src: string }>): string | null {
  const byType = (t: string) => images.find(i => i.type === t)?.src;
  return (
    byType("logo") ||
    byType("logo-svg") ||
    byType("og") ||
    byType("twitter") ||
    byType("favicon") ||
    null
  );
}

// Process raw branding data into BrandingProfile
export function processRawBranding(raw: RawBrandingData): BrandingProfile {
  // Infer palette
  const palette = inferPalette(raw.snapshots, raw.cssData.colors);

  // Typography
  const typography = {
    font_families: {
      primary: raw.typography.stacks.body[0] || "system-ui, sans-serif",
      heading:
        raw.typography.stacks.heading[0] ||
        raw.typography.stacks.body[0] ||
        "system-ui, sans-serif",
    },
    font_stacks: raw.typography.stacks,
    font_sizes: raw.typography.sizes,
  };

  // Spacing
  const baseUnit = inferBaseUnit(raw.cssData.spacings);
  const borderRadius = pickBorderRadius([
    ...raw.snapshots.map(s => s.radius),
    ...raw.cssData.radii,
  ]);

  // Fonts list (all font stacks flattened)
  const allFontStacks = [
    ...Object.values(raw.typography.stacks).flat(),
    ...raw.snapshots.map(s => s.typography.fontStack).flat(),
  ];
  const fontsList = inferFontsList([allFontStacks]);

  // Images
  const images = {
    logo: pickLogo(raw.images),
    favicon: raw.images.find(i => i.type === "favicon")?.src || null,
    og_image:
      raw.images.find(i => i.type === "og")?.src ||
      raw.images.find(i => i.type === "twitter")?.src ||
      null,
  };

  // Components (empty for now - LLM will populate)
  const components = {
    input: {
      border_color: "#CCCCCC",
      border_radius: borderRadius,
    },
  };

  // Button snapshots for LLM
  const buttonSnapshots = raw.snapshots
    .filter(s => s.isButton)
    .slice(0, 20)
    .map((s, idx) => ({
      index: idx,
      text: s.text || "",
      html: "",
      classes: s.classes || "",
      background: s.colors.background || "transparent",
      textColor: s.colors.text || "#000000",
      borderColor:
        s.colors.borderWidth && s.colors.borderWidth > 0
          ? s.colors.border
          : null,
      borderRadius: s.radius ? `${s.radius}px` : "0px",
    }));

  return {
    color_scheme: raw.colorScheme,
    fonts: fontsList,
    colors: palette,
    typography,
    spacing: {
      base_unit: baseUnit,
      border_radius: borderRadius,
    },
    components,
    images,
    __button_snapshots: buttonSnapshots as any,
    __framework_hints: raw.frameworkHints as any,
  };
}

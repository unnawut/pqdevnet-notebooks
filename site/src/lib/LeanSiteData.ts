import yaml from 'js-yaml';
import fs from 'node:fs';
import path from 'node:path';

export interface LeanNotebookConfig {
    id: string;
    title: string;
    description?: string;
    source: string;
    order: number;
    icon?: string;
}

export interface LeanNotebookData {
    html_path: string;
    rendered_at?: string;
    notebook_hash?: string;
    data_hash?: string;
}

export interface LeanManifest {
    latest_devnet?: string;
    devnets?: Record<string, Record<string, LeanNotebookData>>;
    updated_at?: string;
}

export interface LeanPipelineConfig {
    version?: string;
    notebooks: LeanNotebookConfig[];
    settings?: {
        data_dir?: string;
        rendered_dir?: string;
        prometheus_url?: string;
    };
}

/**
 * Data access for Lean Consensus devnet notebooks.
 * Separate from SiteData to avoid conflicts with upstream.
 */
export class LeanSiteData {
    private readonly manifest: LeanManifest;
    private readonly config: LeanPipelineConfig;

    private constructor(manifest: LeanManifest, config: LeanPipelineConfig) {
        this.manifest = manifest;
        this.config = config;
    }

    /**
     * Load Lean site data from filesystem.
     */
    static load(): LeanSiteData {
        const manifest = LeanSiteData.loadManifest();
        const config = LeanSiteData.loadConfig();
        return new LeanSiteData(manifest, config);
    }

    private static loadManifest(): LeanManifest {
        const manifestPath = path.join(process.cwd(), 'rendered', 'manifest.json');
        try {
            if (fs.existsSync(manifestPath)) {
                const content = fs.readFileSync(manifestPath, 'utf-8');
                return JSON.parse(content);
            }
        } catch (e) {
            console.error('Failed to load Lean manifest.json', e);
        }
        return { devnets: {} };
    }

    private static loadConfig(): LeanPipelineConfig {
        const configPath = path.join(process.cwd(), '..', 'lean-pipeline.yaml');
        try {
            const configContent = fs.readFileSync(configPath, 'utf-8');
            return yaml.load(configContent) as LeanPipelineConfig;
        } catch (e) {
            console.error('Failed to load lean-pipeline.yaml', e);
            return { notebooks: [] };
        }
    }

    /** The most recent devnet with rendered data */
    get latestDevnet(): string {
        return this.manifest.latest_devnet || '';
    }

    /** All available devnets, sorted by ID (newest last) */
    get availableDevnets(): string[] {
        if (!this.manifest.devnets) return [];
        return Object.keys(this.manifest.devnets).sort();
    }

    /** Historical devnets (excluding latest), sorted newest first */
    get historicalDevnets(): string[] {
        return this.availableDevnets.filter((d) => d !== this.latestDevnet).reverse();
    }

    /** Notebook configs sorted by order */
    get notebooks(): LeanNotebookConfig[] {
        return [...this.config.notebooks].sort((a, b) => (a.order || 0) - (b.order || 0));
    }

    /** Get notebook data for a specific devnet and notebook ID */
    getNotebookData(devnetId: string, notebookId: string): LeanNotebookData | undefined {
        return this.manifest.devnets?.[devnetId]?.[notebookId];
    }

    /** Check if a devnet has data */
    hasDevnet(devnetId: string): boolean {
        return !!this.manifest.devnets?.[devnetId];
    }

    /** Get notebook config by ID */
    getNotebook(id: string): LeanNotebookConfig | undefined {
        return this.config.notebooks.find((n) => n.id === id);
    }

    /** Check if there's any Lean data available */
    get hasData(): boolean {
        return this.availableDevnets.length > 0;
    }
}

// Singleton instance for helper function compatibility
let leanSiteDataInstance: LeanSiteData | null = null;

function getLeanSiteData(): LeanSiteData {
    if (!leanSiteDataInstance) {
        leanSiteDataInstance = LeanSiteData.load();
    }
    return leanSiteDataInstance;
}

// Helper functions
export function getLeanNotebooks(): LeanNotebookConfig[] {
    return getLeanSiteData().notebooks;
}

export function getLatestDevnet(): string {
    return getLeanSiteData().latestDevnet;
}

export function getAvailableDevnets(): string[] {
    return getLeanSiteData().availableDevnets;
}

export function hasLeanData(): boolean {
    return getLeanSiteData().hasData;
}

import { Activity, Database, Clock, ArrowUpRight } from "lucide-react";

const activityItems: { action: string; detail: string; time: string }[] = [];

const recentQueries: { query: string; count: number }[] = [];

export function ActivityFeed() {
  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 text-primary" aria-hidden="true" />
          <h3 className="text-base font-semibold text-foreground">Activity Feed</h3>
        </div>
        <button className="text-xs font-medium text-primary hover:underline">
          View all
        </button>
      </div>
      {activityItems.length > 0 ? (
        <ul className="space-y-3" aria-label="Recent activity">
          {activityItems.map((item, index) => (
            <li key={index} className="flex items-start gap-3">
              <div className="mt-0.5 h-2 w-2 rounded-full bg-primary/60" aria-hidden="true" />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-foreground">{item.action}</p>
                <p className="truncate text-xs text-muted-foreground">{item.detail}</p>
              </div>
              <span className="shrink-0 text-xs text-muted-foreground">{item.time}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-sm text-muted-foreground text-center py-4">No recent activity.</p>
      )}
    </div>
  );
}

export function IndexUsage() {
  const usagePercent = 0;

  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-4 flex items-center gap-2">
        <Database className="h-4 w-4 text-primary" aria-hidden="true" />
        <h3 className="text-base font-semibold text-foreground">Index Usage</h3>
      </div>
      <div className="space-y-4">
        <div>
          <div className="flex items-end justify-between mb-2">
            <span className="text-2xl font-bold text-foreground">{usagePercent}%</span>
            <span className="text-xs text-muted-foreground">of 10GB</span>
          </div>
          <div
            className="h-2 w-full overflow-hidden rounded-full bg-muted"
            role="progressbar"
            aria-valuenow={usagePercent}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-label="Storage usage"
          >
            <div
              className="h-full rounded-full bg-primary transition-all"
              style={{ width: `${usagePercent}%` }}
            />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3 text-center">
          <div className="rounded-lg bg-muted/50 p-3">
            <p className="text-lg font-semibold text-foreground">0</p>
            <p className="text-xs text-muted-foreground">Documents</p>
          </div>
          <div className="rounded-lg bg-muted/50 p-3">
            <p className="text-lg font-semibold text-foreground">0</p>
            <p className="text-xs text-muted-foreground">Vectors</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export function RecentQueries() {
  return (
    <div className="rounded-2xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-4 flex items-center gap-2">
        <Clock className="h-4 w-4 text-primary" aria-hidden="true" />
        <h3 className="text-base font-semibold text-foreground">Recent Queries</h3>
      </div>
      {recentQueries.length > 0 ? (
        <ul className="space-y-2" aria-label="Recent queries">
          {recentQueries.map((item, index) => (
            <li key={index}>
              <button className="w-full flex items-center justify-between rounded-lg bg-muted/30 px-3 py-2.5 text-left transition-colors hover:bg-muted/50 group">
                <span className="text-sm text-foreground truncate pr-2">{item.query}</span>
                <div className="flex items-center gap-2 shrink-0">
                  <span className="rounded-full bg-primary/10 px-2 py-0.5 text-xs font-medium text-primary">
                    {item.count}x
                  </span>
                  <ArrowUpRight className="h-3 w-3 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" aria-hidden="true" />
                </div>
              </button>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-sm text-muted-foreground text-center py-4">No recent queries.</p>
      )}
    </div>
  );
}

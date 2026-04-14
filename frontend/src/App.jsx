import { useEffect, useMemo, useState } from "react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from "recharts";

const STATUS_ORDER = [
  "full_data",
  "partial_data",
  "no_data",
  "unanalyzed_repo",
  "no_repository",
];

const STATUS_LABELS = {
  full_data: "Full data",
  partial_data: "Partial data",
  no_data: "No data",
  unanalyzed_repo: "Has repo, no README",
  no_repository: "No repository",
};

const STATUS_COLORS = {
  full_data: "#16a34a",
  partial_data: "#eab308",
  no_data: "#dc2626",
  unanalyzed_repo: "#94a3b8",
  no_repository: "#cbd5e1",
};

const BASE = import.meta.env.BASE_URL;

function dataUrl(path) {
  return `${BASE}data/${path}`;
}

function pct(n, total) {
  if (!total) return "";
  return `${((100 * n) / total).toFixed(1)}%`;
}

function StatusBadge({ status }) {
  const color = STATUS_COLORS[status] || "#94a3b8";
  return (
    <span
      className="inline-block px-2 py-0.5 rounded text-xs font-medium text-white"
      style={{ backgroundColor: color }}
    >
      {STATUS_LABELS[status] || status}
    </span>
  );
}

function Overview() {
  const [data, setData] = useState(null);
  useEffect(() => {
    fetch(dataUrl("stats-overview.json"))
      .then((r) => r.json())
      .then(setData);
  }, []);
  if (!data) return <p className="text-gray-500">Loading…</p>;
  const total = data.total_papers;
  return (
    <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
      {STATUS_ORDER.map((s) => {
        const n = data.by_status[s] || 0;
        return (
          <div
            key={s}
            className="p-4 rounded-lg shadow-sm border border-gray-200 bg-white"
          >
            <div className="text-xs text-gray-500 uppercase tracking-wide">
              {STATUS_LABELS[s]}
            </div>
            <div
              className="text-2xl font-bold mt-1"
              style={{ color: STATUS_COLORS[s] }}
            >
              {n.toLocaleString()}
            </div>
            <div className="text-xs text-gray-400">{pct(n, total)}</div>
          </div>
        );
      })}
    </div>
  );
}

function ByYearChart() {
  const [data, setData] = useState([]);
  useEffect(() => {
    fetch(dataUrl("stats-by-year.json"))
      .then((r) => r.json())
      .then(setData);
  }, []);
  if (!data.length) return null;
  return (
    <div className="bg-white p-4 rounded-lg shadow-sm border border-gray-200">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">
        Data availability by year
      </h3>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis dataKey="year" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Legend wrapperStyle={{ fontSize: 11 }} />
          {STATUS_ORDER.map((s) => (
            <Bar
              key={s}
              dataKey={s}
              stackId="status"
              fill={STATUS_COLORS[s]}
              name={STATUS_LABELS[s]}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function ByJournalTable({ yearStart, yearEnd }) {
  const [data, setData] = useState([]);
  useEffect(() => {
    fetch(dataUrl("stats-by-journal.json"))
      .then((r) => r.json())
      .then(setData);
  }, [yearStart, yearEnd]);
  if (!data.length) return null;
  return (
    <div className="bg-white p-4 rounded-lg shadow-sm border border-gray-200">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">
        Status by journal ({yearStart}–{yearEnd})
      </h3>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="text-left text-gray-500 border-b border-gray-200">
              <th className="px-2 py-1 font-semibold">Journal</th>
              <th className="px-2 py-1 font-semibold text-right">Papers</th>
              {STATUS_ORDER.map((s) => (
                <th key={s} className="px-2 py-1 font-semibold text-right">
                  {STATUS_LABELS[s]}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row) => (
              <tr key={row.journal} className="border-b border-gray-100">
                <td className="px-2 py-1.5 text-gray-800">{row.journal}</td>
                <td className="px-2 py-1.5 text-right font-medium">
                  {row.total}
                </td>
                {STATUS_ORDER.map((s) => {
                  const n = row[s] || 0;
                  return (
                    <td key={s} className="px-2 py-1.5 text-right">
                      <span className="text-gray-800">{n}</span>
                      <span className="text-gray-400 text-xs ml-1">
                        ({pct(n, row.total)})
                      </span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PaperBrowser() {
  const [filters, setFilters] = useState({
    year_start: 2019,
    year_end: 2026,
    journal: "",
    status: "",
  });
  const [allPapers, setAllPapers] = useState([]);
  const [journals, setJournals] = useState([]);
  const [page, setPage] = useState(0);
  const [selectedDoi, setSelectedDoi] = useState(null);
  const limit = 25;

  useEffect(() => {
    fetch(dataUrl("journals.json"))
      .then((r) => r.json())
      .then(setJournals);
    fetch(dataUrl("papers.json"))
      .then((r) => r.json())
      .then(setAllPapers);
  }, []);

  const filtered = useMemo(() => {
    return allPapers.filter((p) => {
      if (filters.year_start && p.publication_year < filters.year_start) return false;
      if (filters.year_end && p.publication_year > filters.year_end) return false;
      if (filters.journal && p.journal_name !== filters.journal) return false;
      if (filters.status && p.replication_status !== filters.status) return false;
      return true;
    });
  }, [allPapers, filters]);

  const pageItems = useMemo(() => {
    const start = page * limit;
    return filtered.slice(start, start + limit);
  }, [filtered, page]);

  const total = filtered.length;

  const detail = useMemo(() => {
    if (!selectedDoi) return null;
    return allPapers.find((p) => p.doi === selectedDoi) || null;
  }, [selectedDoi, allPapers]);

  return (
    <div className="bg-white p-4 rounded-lg shadow-sm border border-gray-200">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">
        Browse papers
      </h3>

      <div className="flex flex-wrap gap-3 mb-4 text-sm">
        <div>
          <label className="text-xs text-gray-500 block">Year range</label>
          <div className="flex gap-1">
            <input
              type="number"
              value={filters.year_start}
              onChange={(e) => {
                setFilters({ ...filters, year_start: +e.target.value });
                setPage(0);
              }}
              className="w-20 border border-gray-300 rounded px-2 py-1"
            />
            <span className="self-center">–</span>
            <input
              type="number"
              value={filters.year_end}
              onChange={(e) => {
                setFilters({ ...filters, year_end: +e.target.value });
                setPage(0);
              }}
              className="w-20 border border-gray-300 rounded px-2 py-1"
            />
          </div>
        </div>
        <div>
          <label className="text-xs text-gray-500 block">Journal</label>
          <select
            value={filters.journal}
            onChange={(e) => {
              setFilters({ ...filters, journal: e.target.value });
              setPage(0);
            }}
            className="border border-gray-300 rounded px-2 py-1"
          >
            <option value="">All</option>
            {journals.map((j) => (
              <option key={j} value={j}>
                {j}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="text-xs text-gray-500 block">Status</label>
          <select
            value={filters.status}
            onChange={(e) => {
              setFilters({ ...filters, status: e.target.value });
              setPage(0);
            }}
            className="border border-gray-300 rounded px-2 py-1"
          >
            <option value="">All</option>
            {STATUS_ORDER.map((s) => (
              <option key={s} value={s}>
                {STATUS_LABELS[s]}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="text-xs text-gray-500 mb-2">
        {total.toLocaleString()} matching papers · showing{" "}
        {total > 0 ? page * limit + 1 : 0}–{Math.min((page + 1) * limit, total)}
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="text-left text-gray-500 border-b border-gray-200">
              <th className="px-2 py-1 font-semibold">Year</th>
              <th className="px-2 py-1 font-semibold">Journal</th>
              <th className="px-2 py-1 font-semibold">Title</th>
              <th className="px-2 py-1 font-semibold">Status</th>
            </tr>
          </thead>
          <tbody>
            {pageItems.map((p) => (
              <tr
                key={p.doi}
                className="border-b border-gray-100 cursor-pointer hover:bg-blue-50"
                onClick={() => setSelectedDoi(p.doi)}
              >
                <td className="px-2 py-1.5 text-gray-600">{p.publication_year}</td>
                <td className="px-2 py-1.5 text-gray-600 text-xs">
                  {(p.journal_name || "")
                    .replace("American Economic ", "")
                    .replace("Journal ", "")}
                </td>
                <td className="px-2 py-1.5 text-gray-900">{p.title}</td>
                <td className="px-2 py-1.5">
                  <StatusBadge status={p.replication_status} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex justify-between items-center mt-3 text-sm">
        <button
          disabled={page === 0}
          onClick={() => setPage(page - 1)}
          className="px-3 py-1 border border-gray-300 rounded disabled:opacity-40"
        >
          ← Prev
        </button>
        <span className="text-gray-500">
          Page {page + 1} of {Math.ceil(total / limit) || 1}
        </span>
        <button
          disabled={(page + 1) * limit >= total}
          onClick={() => setPage(page + 1)}
          className="px-3 py-1 border border-gray-300 rounded disabled:opacity-40"
        >
          Next →
        </button>
      </div>

      {selectedDoi && detail && (
        <div
          className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50"
          onClick={() => setSelectedDoi(null)}
        >
          <div
            className="bg-white rounded-lg shadow-xl max-w-3xl w-full max-h-[90vh] overflow-y-auto p-6"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex justify-between items-start gap-4 mb-3">
              <h2 className="text-lg font-bold text-gray-900">{detail.title}</h2>
              <button
                onClick={() => setSelectedDoi(null)}
                className="text-gray-400 hover:text-gray-600 text-xl"
              >
                ×
              </button>
            </div>
            <div className="text-sm text-gray-500 mb-3">
              {detail.journal_name} · {detail.publication_year} ·{" "}
              <a
                href={`https://doi.org/${detail.doi}`}
                target="_blank"
                rel="noreferrer"
                className="text-blue-600 hover:underline"
              >
                {detail.doi}
              </a>
            </div>
            <div className="mb-3">
              <StatusBadge status={detail.replication_status} />
            </div>

            {detail.repositories?.length > 0 && (
              <div className="border-t border-gray-200 pt-3 mt-3">
                <h3 className="text-sm font-semibold text-gray-700 mb-2">
                  Repositories ({detail.repositories.length})
                </h3>
                {detail.repositories.map((r) => (
                  <div
                    key={r.repo_doi}
                    className="bg-gray-50 p-3 rounded mb-2 text-sm"
                  >
                    <div className="flex items-center gap-2 mb-1">
                      <a
                        href={`https://www.openicpsr.org/openicpsr/project/${r.icpsr_project_id}/version/V1/view`}
                        target="_blank"
                        rel="noreferrer"
                        className="text-blue-600 hover:underline font-mono text-xs"
                      >
                        openicpsr/{r.icpsr_project_id}
                      </a>
                      <span className="text-xs text-gray-400">via {r.source}</span>
                    </div>
                    {r.data_availability && (
                      <div className="text-xs text-gray-600">
                        Data:{" "}
                        <span className="font-medium">{r.data_availability}</span>
                        {r.restriction_flags?.length > 0 && (
                          <span className="ml-2">
                            Flags: {r.restriction_flags.join(", ")}
                          </span>
                        )}
                      </div>
                    )}
                    {r.readme_text && (
                      <details className="mt-2">
                        <summary className="text-xs text-gray-500 cursor-pointer hover:text-gray-700">
                          README excerpt ({r.readme_text.length} chars)
                        </summary>
                        <pre className="text-xs text-gray-700 bg-white p-2 mt-1 rounded border border-gray-200 max-h-64 overflow-y-auto whitespace-pre-wrap">
                          {r.readme_text.substring(0, 2000)}
                        </pre>
                      </details>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default function App() {
  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <h1 className="text-xl font-bold text-gray-900">
            AEA Replication Tracker
          </h1>
          <p className="text-sm text-gray-500 mt-0.5">
            Data availability across AEA journal articles
          </p>
        </div>
      </header>

      <main className="max-w-7xl mx-auto p-6 space-y-6">
        <Overview />
        <ByYearChart />
        <ByJournalTable yearStart={2019} yearEnd={2026} />
        <PaperBrowser />
      </main>
    </div>
  );
}

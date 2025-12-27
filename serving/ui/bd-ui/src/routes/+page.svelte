<script lang="js">
  import { onMount } from "svelte";
  import TimeSeriesChart from "$lib/TimeSeriesChart.svelte";

  // State
  let data = $state([]); 
  let availableMetrics = $state([]); 
  let selectedMetric = $state(""); 
  let failureCount = $state(0);
  const maxFailures = 10;

  // HDFS State
  let report = $state(null);
  let loadingReport = $state(false);

  // ---- INITIAL SETUP ----
  onMount(async () => {
    await fetchAvailableMetrics();
    fetchData(); 
  });

  async function fetchAvailableMetrics() {
    try {
      const res = await fetch("http://localhost:3000/metrics");
      const json = await res.json();
      if (json.meterological_observations) {
        availableMetrics = json.meterological_observations;
        if (availableMetrics.length > 0) {
          selectedMetric = availableMetrics[0];
        }
      }
    } catch (err) {
      console.error("Failed to load metrics:", err);
    }
  }

  // ---- KAFKA DATA POLLING ----
  async function fetchData() {
    try {
      const res = await fetch("http://localhost:3000/data");
      if (!res.ok) throw new Error("Network response not ok");
      
      const json = await res.json();
      const meteo = json.meterological_observations || {};

      const newPoint = {
        timestamp: new Date().toISOString(),
      };

      availableMetrics.forEach(metric => {
        newPoint[metric] = meteo[metric]?.value ?? 0;
      });

      data = [...data.slice(-29), newPoint];
      failureCount = 0;
    } catch (err) {
      console.error("Fetch error:", err);
      failureCount += 1;
    }
  }

  // ---- HDFS REPORT FETCHING ----
  async function fetchHdfsReport() {
    loadingReport = true;
    try {
      const res = await fetch("http://localhost:3000/reports/latest");
      const rawReport = await res.json();

      // Everything related to rawReport must stay inside this block
      if (rawReport.data) {
        rawReport.data.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
      }
      
      // Update the global state
      report = rawReport; 
    } catch (e) {
      console.error("HDFS Fetch failed", e);
    } finally {
      loadingReport = false;
    }
  }

  // Polling interval
  $effect(() => {
    const interval = setInterval(() => {
      if (failureCount < maxFailures) fetchData();
    }, 4000);
    return () => clearInterval(interval);
  });

  const getLatestValue = (metric) => {
    return data.length > 0 ? data.at(-1)[metric] : "—";
  };
</script>

<div class="min-h-screen bg-gray-900 text-gray-100 p-6">
  <header class="mb-8">
    <h1 class="text-3xl font-bold text-white">Big Data Energy Dashboard</h1>
    <p class="text-gray-400">Live Kafka Stream: Meteorological Observations</p>
  </header>

  <section class="mb-10">
    <h2 class="text-2xl font-semibold text-white mb-4">Latest Observations</h2>
    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
      {#each availableMetrics.slice(0, 10) as metric}
        <div class="bg-gray-800 p-6 rounded-xl shadow border border-gray-700">
          <h3 class="text-sm font-medium text-gray-400 uppercase tracking-wider">{metric}</h3>
          <p class="text-3xl font-bold text-blue-400 mt-2">
            {getLatestValue(metric)}
          </p>
        </div>
      {/each}
    </div>
  </section>

  <section class="mb-10">
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-2xl font-semibold text-white">Live Trends</h2>
      
      <div class="flex items-center gap-3">
        <span class="text-sm text-gray-400">Select Metric:</span>
        <select
          bind:value={selectedMetric}
          class="bg-gray-800 border border-gray-700 rounded-lg px-4 py-2 text-white focus:ring-2 focus:ring-blue-500 outline-none"
        >
          {#each availableMetrics as metric}
            <option value={metric}>{metric}</option>
          {/each}
        </select>
      </div>
    </div>

    <div class="bg-gray-800 rounded-xl p-6 border border-gray-700">
      {#if data.length > 1}
        <div class="h-80 w-full">
           <TimeSeriesChart {data} variable={selectedMetric} />
        </div>
      {:else}
        <div class="h-80 flex items-center justify-center text-gray-500 italic">
          Collecting stream data...
        </div>
      {/if}
    </div>
  </section>

<section class="mt-10 bg-gray-900 border border-slate-800 rounded-xl p-5">
  <div class="flex justify-between items-center mb-4">
    <div>
      <h2 class="text-lg font-bold text-white">Prediction Reports</h2>
      <p class="text-xs text-slate-500">HDFS Batch Accuracy</p>
      <p class="text-xs text-slate-500">Historical prediction vs. actual totals</p>
    </div>
    <button 
      onclick={fetchHdfsReport}
      class="bg-blue-600 px-3 py-1.5 rounded text-xs font-semibold hover:bg-blue-500 transition"
      disabled={loadingReport}
    >
      {loadingReport ? 'Retrieving...' : 'Get Latest Report'}
    </button>
  </div>

  {#if report}
    <div class="max-h-64 overflow-y-auto border border-slate-800 rounded-lg custom-scrollbar">
      <table class="w-full text-left text-xs">
        <thead class="sticky top-0 bg-slate-800 text-slate-400 uppercase text-[9px] tracking-wider z-10">
          <tr>
            <th class="px-4 py-2">Date</th>
            <th class="px-4 py-2 text-right">Pred (M)</th>
            <th class="px-4 py-2 text-right">Act (k)</th>
            <th class="px-4 py-2 text-right">Diff</th>
            <th class="px-4 py-2 text-center">Status</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-800">
          {#each report.data as row}
            <tr class="hover:bg-slate-800/40">
              <td class="px-4 py-2 text-slate-400 whitespace-nowrap">
                {new Date(row.timestamp).toLocaleDateString('da-DK', { month: '2-digit', day: '2-digit' })}
              </td>
              <td class="px-4 py-2 text-right font-mono text-blue-400">
                {(row.predicted_total / 1000000).toFixed(1)}M
              </td>
              <td class="px-4 py-2 text-right font-mono text-emerald-400">
                {(row.actual_total / 1000).toFixed(0)}k
              </td>
              <td class="px-4 py-2 text-right font-mono text-slate-500">
                {Math.abs(row.diff_kwh / 1000000).toFixed(1)}M
              </td>
              <td class="px-4 py-2 text-center">
                <div class="h-2 w-2 rounded-full bg-red-500 mx-auto shadow-[0_0_8px_rgba(239,68,68,0.5)]" title="High Skew"></div>
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {:else}
    <div class="text-center py-8 text-slate-600 text-sm border-2 border-dashed border-slate-800 rounded-lg">
      Click 'Refresh' to load HDFS data.
    </div>
  {/if}
</section>

  <footer class="grid grid-cols-1 mt-6md:grid-cols-2 gap-6 opacity-60">
     <div class="bg-gray-800 p-4 rounded-lg text-xs">
        <p>Status: {failureCount > 0 ? 'Reconnecting...' : 'Connected to Kafka'}</p>
        <p>Active Metrics: {availableMetrics.join(', ')}</p>
     </div>
  </footer>
</div>

<style>
  /* Optional: Make the scrollbar look "Engineered" */
  .custom-scrollbar::-webkit-scrollbar { width: 6px; }
  .custom-scrollbar::-webkit-scrollbar-track { background: #111827; }
  .custom-scrollbar::-webkit-scrollbar-thumb { background: #374151; border-radius: 10px; }
</style>
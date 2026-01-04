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

  // Setup on mount
  onMount(async () => {
    await fetchAvailableMetrics();
    fetchData(); 
  });

  async function fetchAvailableMetrics() {
    try {
      const res = await fetch("http://localhost:3000/metrics");
      const json = await res.json();
      if (json.meterological_observations) {
        // Stable alphabetical sorting
        availableMetrics = json.meterological_observations.sort();
        if (availableMetrics.length > 0) {
          selectedMetric = availableMetrics[0];
        }
      }
    } catch (err) {
      console.error("Failed to load metrics:", err);
    }
  }

  // KAFKA DATA POLLING
  async function fetchData() {
    try {
      const res = await fetch("http://localhost:3000/data");
      const json = await res.json();
      const meteo = json.meterological_observations || {};
      const newPoint = { timestamp: new Date().toISOString() };

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

  async function fetchHdfsReport() {
    loadingReport = true;
    try {
      const res = await fetch("http://localhost:3000/reports/latest");
      const rawReport = await res.json();
      if (rawReport.data) {
        rawReport.data.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
      }
      report = rawReport; 
    } catch (e) {
      console.error("HDFS Fetch failed", e);
    } finally {
      loadingReport = false;
    }
  }

  $effect(() => {
    const interval = setInterval(() => {
      if (failureCount < maxFailures) fetchData();
    }, 4000);
    return () => clearInterval(interval);
  });

  const getLatestValue = (metric) => {
    return data.length > 0 ? data.at(-1)[metric]?.toFixed(1) : "—";
  };
</script>

<div class="min-h-screen bg-gray-800 text-gray-100 p-6">
  <header class="mb-8">
    <h1 class="text-3xl font-bold text-white">Big Data Energy Dashboard</h1>
    <p class="text-gray-300">Live Kafka Stream: Meteorological Observations</p>
  </header>

  <section class="mb-10">
    <h2 class="text-2xl font-semibold text-white mb-4">Latest Observations</h2>
    <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4">
      {#each availableMetrics.slice(0, 10) as metric}
        <div class="bg-gray-700 p-4 rounded-xl shadow border border-gray-600">
          <h3 class="text-xs font-medium text-gray-400 uppercase tracking-wider">{metric}</h3>
          <p class="text-2xl font-bold text-blue-400 mt-1">
            {getLatestValue(metric)}
          </p>
        </div>
      {/each}
    </div>
  </section>

  <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
    
    <section class="lg:col-span-2">
      <div class="flex justify-between items-center mb-4">
        <h2 class="text-2xl font-semibold text-white">Live Trends</h2>
        <div class="flex items-center gap-3">
          <span class="text-sm text-gray-400">Metric:</span>
          <select
            bind:value={selectedMetric}
            class="bg-gray-700 border border-gray-600 rounded-lg px-3 py-1 text-white text-sm outline-none"
          >
            {#each availableMetrics as metric}
              <option value={metric}>{metric}</option>
            {/each}
          </select>
        </div>
      </div>

      <div class="bg-gray-700 rounded-xl p-6 border border-gray-600">
        {#if data.length > 1}
          <div class="h-80 w-full">
             <TimeSeriesChart {data} variable={selectedMetric} />
          </div>
        {:else}
          <div class="h-80 flex items-center justify-center text-gray-400 italic">
            Connecting to Kafka stream...
          </div>
        {/if}
      </div>
    </section>

    <section>
      <div class="flex justify-between items-center mb-4">
        <h2 class="text-2xl font-semibold text-white">Prediction Reports</h2>
        <button 
          onclick={fetchHdfsReport}
          class="bg-blue-600 px-3 py-1 rounded text-xs font-bold hover:bg-blue-500 transition"
          disabled={loadingReport}
        >
          {loadingReport ? 'Fetching...' : 'Fetch Latest Report'}
        </button>
      </div>

      <div class="bg-gray-700 border border-gray-600 rounded-xl p-4 h-[410px] flex flex-col">
        <div class="mb-3">
          <p class="text-xs text-gray-400 font-bold uppercase tracking-tight">HDFS Batch Accuracy</p>
          <p class="text-[10px] text-gray-500">Historical prediction vs. actual totals</p>
        </div>

        {#if report}
          <div class="flex-grow overflow-y-auto custom-scrollbar border border-gray-600 rounded-lg">
            <table class="w-full text-left text-[11px]">
              <thead class="sticky top-0 bg-gray-600 text-gray-300 uppercase text-[9px] tracking-wider">
                <tr>
                  <th class="px-3 py-2">Date</th>
                  <th class="px-2 py-2 text-right">Est. (M)</th>
                  <th class="px-2 py-2 text-right">Act. (k)</th>
                  <th class="px-2 py-2 text-right text-orange-400">Gap (M)</th>
                  <th class="px-2 py-2 text-center">Status</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-600">
                {#each report.data as row}
                  {@const diff = (row.predicted_total - (row.actual_total * 1000))}
                  <tr class="hover:bg-gray-600/50">
                    <td class="px-3 py-2 text-gray-300 whitespace-nowrap">
                      {new Date(row.timestamp).toLocaleDateString('da-DK', { month: '2-digit', day: '2-digit' })}
                    </td>
                    <td class="px-2 py-2 text-right font-mono text-blue-300">
                      {(row.predicted_total / 1000000).toFixed(1)}M
                    </td>
                    <td class="px-2 py-2 text-right font-mono text-emerald-300">
                      {(row.actual_total / 1000).toFixed(0)}k
                    </td>
                    <td class="px-2 py-2 text-right font-mono {diff > 0 ? 'text-orange-300' : 'text-red-300'}">
                      {Math.abs(diff / 1000000).toFixed(1)}M
                    </td>
                    <td class="px-2 py-2 text-center">
                      <span 
                        class="px-1.5 py-0.5 rounded-full text-[8px] font-bold uppercase 
                        {Math.abs(diff) > 1000000 ? 'bg-red-900/40 text-red-400 border border-red-700' : 'bg-emerald-900/40 text-emerald-400 border border-emerald-700'}"
                      >
                        {Math.abs(diff) > 1000000 ? 'Skew' : 'Valid'}
                      </span>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else}
          <div class="flex-grow flex items-center justify-center border-2 border-dashed border-gray-600 rounded-lg">
            <p class="text-gray-500 text-xs italic">Awaiting HDFS Data</p>
          </div>
        {/if}
      </div>
    </section>

  </div>

  <footer class="mt-10 pt-6 border-t border-gray-700 text-gray-500 text-xs flex justify-between">
    <div class="flex gap-4">
      <p>Kafka Status: <span class={failureCount > 0 ? 'text-red-400' : 'text-emerald-400'}>{failureCount > 0 ? 'Error' : 'Stable'}</span></p>
      <p>HDFS Path: /reports/accuracy_avro</p>
    </div>
    <p>Big Data Engineering Demo</p>
  </footer>
</div>

<style>
  .custom-scrollbar::-webkit-scrollbar { width: 4px; }
  .custom-scrollbar::-webkit-scrollbar-track { background: #374151; }
  .custom-scrollbar::-webkit-scrollbar-thumb { background: #4b5563; border-radius: 10px; }
</style>
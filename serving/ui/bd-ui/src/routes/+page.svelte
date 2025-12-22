<script lang="js">
  import TimeSeriesChart from "$lib/TimeSeriesChart.svelte";

  let data = $state([]);
  let useMockData = $state(true);
  let failureCount = $state(0);
  const maxFailures = 3;


  // ---- MOCK DATA ----
  function generateMockData() {
    const now = new Date();
    return {
      timestamp: now.toISOString(),
      temperature: +(15 + Math.random() * 10).toFixed(1),
      windSpeed: +(2 + Math.random() * 5).toFixed(1),
      energy: +(500 + Math.random() * 200).toFixed(0),
      tempPrediction: +(15 + Math.random() * 10).toFixed(1),
      energyForecast: +(500 + Math.random() * 200).toFixed(0)
    };
  }

  // ---- FETCH ----
  async function fetchData() {
    if (useMockData) {
      data = [...data.slice(-19), generateMockData()];
      failureCount = 0;
      return;
    }

    try {
      const res = await fetch("/api/data");
      if (!res.ok) throw new Error("Network response was not ok");
      const json = await res.json();
      data = json;
      failureCount = 0;
    } catch (err) {
      failureCount += 1;
    }
  }

  // ---- POLL ----
  $effect(() => {
   
    const interval = setInterval(() => {
      if (failureCount >= maxFailures) {
        clearInterval(interval);
      } else {
        fetchData();
      }
    }, 4000);
    return () => clearInterval(interval);
  });

   fetchData();

  // ---- DROPDOWN VARIABLES ----
 /* 	let variables = [
		"temperature", "windSpeed", "energy", "tempPrediction", "energyForecast"
	];
  */  let variables = [
    { id:"temperature", label: "Temperature (°C)" },
    { id:"windSpeed", label: "Wind Speed (m/s)" },
    { id:"energy", label: "Energy Use (kWh)" },
    { id:"tempPrediction", label: "Temperature Prediction (°C)" },
    { id:"energyForecast", label: "Energy Forecast (kWh)" }
  ];

  let selected = $state(variables[0]);
  
  let test = $state("");

</script>

<div class="min-h-screen bg-gray-900 text-gray-100 p-6">

  <!-- HEADER -->
  <header class="mb-8 flex justify-between items-start">
    <div class="flex flex-col gap-1">
      <h1 class="text-3xl font-bold text-white">Big Data Energy Dashboard</h1>
      <p class="text-gray-400">Live DMI Observations + SPARKS Predictions</p>
    </div>

    <label class="flex items-center gap-3">
      <span class="text-gray-200">Use Mock Data</span>
      <input
        type="checkbox"
        class="toggle toggle-primary"
        checked={useMockData}
        onchange={(e) => useMockData.set(e.currentTarget.checked)}
      />
    </label>
  </header>

  <!-- ─────────────────────────────────────────── -->
  <!--  1. CURRENT OBSERVATIONS                    -->
  <!-- ─────────────────────────────────────────── -->
  <section class="mb-10">
    <h2 class="text-2xl font-semibold text-white mb-4">Current Observations</h2>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
      <!-- Temperature -->
      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <h3 class="text-xl font-semibold text-gray-200">Temperature (°C)</h3>
        <p class="text-3xl font-bold text-yellow-400 mt-2">
          {data.length ? data.at(-1).temperature : "—"}
        </p>
      </div>

      <!-- Wind -->
      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <h3 class="text-xl font-semibold text-gray-200">Wind Speed (m/s)</h3>
        <p class="text-3xl font-bold text-blue-400 mt-2">
          {data.length ? data.at(-1).windSpeed : "—"}
        </p>
      </div>

      <!-- Energy -->
      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <h3 class="text-xl font-semibold text-gray-200">Energy Use (kWh)</h3>
        <p class="text-3xl font-bold text-green-400 mt-2">
          {data.length ? data.at(-1).energy : "—"}
        </p>
      </div>
    </div>
  </section>

  <!-- ─────────────────────────────────────────── -->
  <!--  2. PREDICTIONS & MODEL OUTPUT              -->
  <!-- ─────────────────────────────────────────── -->
  <section class="mb-10">
    <h2 class="text-2xl font-semibold text-white mb-4">Predictions</h2>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
      <!-- Energy Forecast -->
      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <h3 class="text-xl font-semibold text-gray-200">
          SPARKS Energy Forecast (kWh)
        </h3>
        <p class="text-3xl font-bold text-green-300 mt-2">
          {data.length ? data.at(-1).energyForecast : "—"}
        </p>
      </div>

      <!-- Temp Forecast -->
      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <h3 class="text-xl font-semibold text-gray-200">
          Temperature Prediction (°C)
        </h3>
        <p class="text-3xl font-bold text-yellow-300 mt-2">
          {data.length ? data.at(-1).tempPrediction : "—"}
        </p>
      </div>
    </div>
  </section>

  <!-- ─────────────────────────────────────────── -->
  <!--  3. TIME SERIES GRAPHS                      -->
  <!-- ─────────────────────────────────────────── -->

  <div class="mb-6 bg-gray-800 rounded-xl p-4 flex items-center justify-center h-32 text-gray-400">
      <p>Debugging: Failurecount: {failureCount}</p>
    </div>

  <section class="mb-10">
    <div class="flex justify-between mb-3">
      <h2 class="text-2xl font-semibold text-white">Trends</h2>

      <select
        bind:value={selected.id}
        class="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1"
      >
        {#each variables as v}
          <option value={v.id}>
            {v.label}
          </option>
        {/each}
        
      </select>
    
    </div>


    <input bind:value={test} />
    <div class="bg-gray-800 rounded-xl p-4 flex items-center justify-center h-64 text-gray-400">
      <!-- Replace with chart component -->
      
      Time-series chart for: {selected.id}
      <div class="bg-gray-800 rounded-xl p-4 h-64">
        <TimeSeriesChart data={data} variable={selected.id} />
      </div>
    </div>
  </section>

  <!-- ─────────────────────────────────────────── -->
  <!--  4. BIG DATA OVERVIEW                       -->
  <!-- ─────────────────────────────────────────── -->
  <section>
    <h2 class="text-2xl font-semibold text-white mb-4">Big Data Overview</h2>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <p class="text-gray-400">Meteorological Dataset</p>
        <p class="text-2xl font-semibold text-white">1.39 GiB</p>
      </div>

      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <p class="text-gray-400">Energy Dataset</p>
        <p class="text-2xl font-semibold text-white">890 MiB</p>
      </div>

      <div class="bg-gray-800 p-6 rounded-xl shadow">
        <p class="text-gray-400">Processed Data Points</p>
        <p class="text-2xl font-semibold text-white">Millions+</p>
      </div>
    </div>
  </section>
</div>

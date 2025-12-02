<script lang="javascript">
  let data = $state([]);
  let useMockData = $state(true);
  let failureCount = $state(0);
  const maxFailures = 3;

  // Mock data generator
  function generateMockData() {
    const now = new Date();
    return {
      timestamp: now.toISOString(),
      temperature: +(15 + Math.random() * 10).toFixed(1),
      windSpeed: +(2 + Math.random() * 5).toFixed(1),
      energy: +(500 + Math.random() * 200).toFixed(0),
      tempPrediction: +(15 + Math.random() * 10).toFixed(1),
      energyForecast: +(500 + Math.random() * 200).toFixed(0),
    };
  }

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
      console.error("Failed to fetch data:", err);
      failureCount += 1;
    }
  }

  // Polling effect
  $effect(() => {
    fetchData();
    const interval = setInterval(() => {
      if (failureCount >= maxFailures) {
        console.warn("Max fetch failures reached. Stopping fetch.");
        clearInterval(interval);
      } else {
        fetchData();
      }
    }, 5000);

    return () => clearInterval(interval);
  });
</script>

<div class="min-h-screen bg-gray-900 text-gray-100 p-6">
  <header class="mb-6 flex justify-between items-center">
    <div class="flex flex-col gap-2">
      <h1 class="text-3xl font-bold text-white">Big Data Energy</h1>
      <h2 class="text-1.5xl text-gray-300">DMI & EnergiFyn Dashboard</h2>
    </div>
    <label class="flex items-center gap-2">
      <span class="text-gray-200">Use Mock Data</span>
      <input
        type="checkbox"
        checked={useMockData}
        onchange={(e) => useMockData.set(e.currentTarget.checked)}
        class="toggle toggle-primary"
      />
    </label>
  </header>

  <main class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
    <div class="bg-gray-800 shadow rounded-lg p-6">
      <h2 class="text-xl font-semibold text-gray-200">Temperature (°C)</h2>
      <p class="text-3xl font-bold text-yellow-400 mt-2">
        {data.length > 0 ? data[data.length - 1].temperature : "Loading..."}
      </p>
      <div class="h-2 w-full bg-gray-700 rounded mt-2">
        <div
          class="h-2 bg-yellow-400 rounded"
          style="width: {data.length > 0
            ? Math.min(data[data.length - 1].temperature * 5, 100) + '%'
            : '0%'}"
        ></div>
      </div>
    </div>

    <div class="bg-gray-800 shadow rounded-lg p-6">
      <h2 class="text-xl font-semibold text-gray-200">Wind Speed (m/s)</h2>
      <p class="text-2xl text-blue-400 mt-2">
        {data.length > 0 ? data[data.length - 1].windSpeed : "Loading..."}
      </p>
      <div class="h-2 w-full bg-gray-700 rounded mt-2">
        <div
          class="h-2 bg-blue-400 rounded"
          style="width: {data.length > 0
            ? Math.min(data[data.length - 1].windSpeed * 15, 100) + '%'
            : '0%'}"
        ></div>
      </div>
    </div>

    <div class="bg-gray-800 shadow rounded-lg p-6">
      <h2 class="text-xl font-semibold text-gray-200">
        Energy Consumption (kWh)
      </h2>
      <p class="text-2xl text-green-400 mt-2">
        {data.length > 0 ? data[data.length - 1].energy : "Loading..."}
      </p>
      <div class="h-2 w-full bg-gray-700 rounded mt-2">
        <div
          class="h-2 bg-green-400 rounded"
          style="width: {data.length > 0
            ? Math.min(data[data.length - 1].energy / 8, 100) + '%'
            : '0%'}"
        ></div>
      </div>
    </div>
  </main>

  <section class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
    <div class="bg-gray-800 shadow rounded-lg p-6">
      <h2 class="text-xl font-semibold text-gray-200">
        SPARKS Energy Prediction (kWh)
      </h2>
      <p class="text-2xl text-green-300 mt-2">
        {data.length > 0 ? data[data.length - 1].energyForecast : "Loading..."}
      </p>
      <div class="h-2 w-full bg-gray-700 rounded mt-2">
        <div
          class="h-2 bg-green-300 rounded"
          style="width: {data.length > 0
            ? Math.min(data[data.length - 1].energyForecast / 8, 100) + '%'
            : '0%'}"
        ></div>
      </div>
    </div>

    <div class="bg-gray-800 shadow rounded-lg p-6">
      <h2 class="text-xl font-semibold text-gray-200">
        Temperature Prediction (°C)
      </h2>
      <p class="text-2xl text-yellow-300 mt-2">
        {data.length > 0 ? data[data.length - 1].tempPrediction : "Loading..."}
      </p>
      <div class="h-2 w-full bg-gray-700 rounded mt-2">
        <div
          class="h-2 bg-yellow-300 rounded"
          style="width: {data.length > 0
            ? Math.min(data[data.length - 1].tempPrediction * 5, 100) + '%'
            : '0%'}"
        ></div>
      </div>
    </div>
  </section>

  <section class="mb-8">
    <h2 class="text-xl font-semibold text-gray-200 mb-4">
      Energy Usage vs SPARKS Prediction
    </h2>
    <div
      class="w-full h-48 bg-gray-800 rounded-lg flex items-center justify-center text-gray-400"
    >
      Chart placeholder
    </div>
  </section>

  <section>
    <h2 class="text-xl font-semibold text-gray-200 mb-4">Temperature Trends</h2>
    <div
      class="w-full h-48 bg-gray-800 rounded-lg flex items-center justify-center text-gray-400"
    >
      Chart placeholder
    </div>
  </section>
</div>

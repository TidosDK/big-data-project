<script lang="js">
  import * as echarts from "echarts";
  import { onMount, tick } from "svelte";

  export let data = [];
  export let variable = "temperature";

  let chartDiv;
  let chart;

  onMount(async () => {
    await tick();
    chart = echarts.init(chartDiv);

    const resizeObserver = new ResizeObserver(() => chart.resize());
    resizeObserver.observe(chartDiv);

    return () => {
      resizeObserver.disconnect();
      chart.dispose();
    };
  });

  $: if (chart && data.length > 0) {
    chart.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "time",
        data: data.map((d) => d.timestamp),
        axisLabel: {
          color: "#ccc",
          formatter: {
            // ECharts will automatically pick the best format
            primary: '{HH}:{mm}:{ss}' 
          }
        }
      },
      yAxis: {
        type: "value",
        axisLabel: { color: "#ccc" }
      },
      grid: { left: 40, right: 20, top: 20, bottom: 40 },
      series: [
        {
          name: variable,
          type: "line",
          smooth: true,
          symbol: "none",
          // Map to [timestamp, value] array for 'time' axis
          data: data.map((d) => [d.timestamp, d[variable]]),
          lineStyle: { width: 2, color: "#4ade80" }
        }
      ]
    });
  }
</script>

<div bind:this={chartDiv} class="w-full h-full min-h-[250px]"></div>

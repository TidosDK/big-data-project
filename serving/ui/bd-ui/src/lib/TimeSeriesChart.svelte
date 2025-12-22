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
        type: "category",
        data: data.map((d) => d.timestamp),
        axisLabel: {
          color: "#ccc",
          formatter: (value) =>
            new Date(value).toLocaleTimeString("da-DK", {
              hour: "2-digit",
              minute: "2-digit"
            })
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
          data: data.map((d) => d[variable]),
          lineStyle: { width: 2, color: "#4ade80" }
        }
      ]
    });
  }
</script>

<div bind:this={chartDiv} class="w-full h-full min-h-[250px]"></div>

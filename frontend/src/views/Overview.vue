<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <section v-if="resumeItems.length" class="resume-board">
      <h3>工作台 · 断点续做（{{ resumeItems.length }}）</h3>
      <p class="page-desc">以下任务在执行窗口结束时只完成了当前批次并记录断点，下一轮将从断点水位继续，而不是重跑旧水位。</p>
      <table class="data-table">
        <thead>
          <tr><th>任务编号</th><th>关联源端</th><th>断点水位</th><th>轮次</th><th>已采集条数</th><th>窗口（秒）</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in resumeItems" :key="item.task_no">
            <td>{{ item.task_no }}</td>
            <td>{{ item.source }}</td>
            <td>{{ item.watermark }}</td>
            <td>{{ item.round_no }}</td>
            <td>{{ item.fetched }}</td>
            <td>{{ item.window_seconds }} / 已用 {{ item.consumed_seconds }}</td>
          </tr>
        </tbody>
      </table>
    </section>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

type ResumeItem = {
  task_no: string
  source: string
  watermark: number
  round_no: string
  fetched: number
  window_seconds: number
  consumed_seconds: number
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])
const resumeItems = ref<ResumeItem[]>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "采集源管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "网页抓取规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集任务", "created": 0, "pending": 0, "abnormal": 0}, {"name": "调度编排", "created": 0, "pending": 0, "abnormal": 0}, {"name": "增量采集游标", "created": 0, "pending": 0, "abnormal": 0}, {"name": "RPA流程配置", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集日志", "created": 0, "pending": 0, "abnormal": 0}, {"name": "失败重试策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "代理与请求控制", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据解析模板", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集去重", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集版本管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集告警", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集看板", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集数据存储", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集审计", "created": 0, "pending": 0, "abnormal": 0}]
  }
  try {
    const workbench = await fetchJson<{ total: number; items: ResumeItem[] }>('/api/task/workbench/resume')
    resumeItems.value = workbench.items
  } catch {
    resumeItems.value = []
  }
})
</script>

<style scoped>
.resume-board {
  margin: 8px 0 20px;
  padding: 12px 16px;
  border: 1px solid #d7deea;
  border-radius: 10px;
  background: #f7f9fc;
}

.resume-board h3 {
  margin: 0 0 6px;
  font-size: 15px;
}
</style>

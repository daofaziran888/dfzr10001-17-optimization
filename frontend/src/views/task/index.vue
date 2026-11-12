<template>
  <section class="page" data-module="task">
    <header class="page-head">
      <div>
        <h2>采集任务管理</h2>
        <p class="page-desc">维护采集任务的调度周期、增量策略与执行参数；临近窗口结束只收尾当前批次并落断点，下一轮从断点续做。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记采集任务</button>
        <button class="btn" type="button" @click="exportRows">导出采集任务清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <!-- 工作台续做条目：窗口结束、断点挂起的任务集中在这里，一键续做 -->
    <section v-if="resumeItems.length" class="resume-board">
      <h3>工作台 · 断点续做（{{ resumeItems.length }}）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>任务编号</th>
            <th>关联源端</th>
            <th>断点水位</th>
            <th>增量字段</th>
            <th>上轮批次</th>
            <th>窗口/已用（秒）</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in resumeItems" :key="item.task_no">
            <td>{{ item.task_no }}</td>
            <td>{{ item.source }}</td>
            <td>{{ item.watermark }}</td>
            <td>{{ item.increment_field }}</td>
            <td>{{ item.round_no }} · {{ item.fetched }} 条</td>
            <td>{{ item.window_seconds }} / {{ item.consumed_seconds }}</td>
            <td class="row-actions">
              <button class="link" type="button" :disabled="busy === item.task_no" @click="resumeTask(item.task_no)">
                {{ busy === item.task_no ? '续做中…' : '从断点续做' }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>状态</th>
          <th>上次进度（断点 / 游标 / 最近批次）</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>
            {{ row.status }}
            <span v-if="row.has_resume" class="resume-tag">待续做</span>
          </td>
          <td class="progress-cell" @click="openDetail(row)">
            <template v-if="row.progress?.checkpoint">
              断点 {{ row.progress.checkpoint.watermark }} ·
              游标 {{ row.progress.cursor?.当前水位 ?? '—' }} ·
              日志 {{ row.progress.latest_log?.日志编号 ?? '—' }}
              （{{ row.progress.latest_log?.关联源端 ?? '—' }} /
              {{ row.progress.latest_log?.采集行数 ?? 0 }} 条）
            </template>
            <span v-else>尚未采集</span>
          </td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" :disabled="busy === row.任务编号" @click="collectTask(row)">
              {{ busy === row.任务编号 ? '采集中…' : '跑一个窗口' }}
            </button>
            <button class="link" type="button" @click="syncSchedule(row)">同步调度计划</button>
            <button class="link" type="button" @click="openDetail(row)">任务详情</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 3" class="empty-state">暂无采集任务数据，可先登记采集任务</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集任务记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-if="successMessage" class="success-text">{{ successMessage }}</span>
    </footer>

    <!-- 任务详情：断点、游标、最近批次日志必须对应同一源端与同一批次 -->
    <div v-if="detail" class="drawer-mask" @click.self="detail = null">
      <div class="drawer">
        <header class="drawer-head">
          <h3>{{ detail.任务编号 }} · 任务详情</h3>
          <button class="link" type="button" @click="detail = null">关闭</button>
        </header>

        <div class="detail-grid">
          <p><b>关联源端：</b>{{ detail.关联源端 }}</p>
          <p><b>增量字段：</b>{{ detail.增量字段 }}</p>
          <p><b>起始水位：</b>{{ detail.起始水位 }}</p>
          <p><b>批次大小：</b>{{ detail.批次大小 }}</p>
          <p><b>执行窗口（秒）：</b>{{ detail.执行窗口 }}</p>
          <p><b>当前状态：</b>{{ detail.status }}</p>
        </div>

        <h4>调度计划（任务页同步口径）</h4>
        <p v-if="detail.schedule">
          {{ detail.schedule.计划编号 }} · {{ detail.schedule.调度表达式 }} ·
          超时 {{ detail.schedule.超时阈值 }} 秒 · {{ detail.schedule.status }}
        </p>
        <p v-else class="empty-state">尚未关联调度计划</p>

        <h4>断点 / 游标 / 最近批次一致性</h4>
        <table class="data-table">
          <thead>
            <tr><th>口径</th><th>源端</th><th>水位 / 编号</th><th>批次</th><th>采集时间</th></tr>
          </thead>
          <tbody>
            <tr v-if="detail.progress?.checkpoint">
              <td>断点</td>
              <td>{{ detail.progress.checkpoint.source }}</td>
              <td>{{ detail.progress.checkpoint.watermark }}（{{ detail.progress.checkpoint.state }}）</td>
              <td>{{ detail.progress.checkpoint.round_no }}</td>
              <td>—</td>
            </tr>
            <tr v-if="detail.progress?.cursor">
              <td>增量游标</td>
              <td>{{ detail.progress.cursor.来源标识 }}</td>
              <td>上次 {{ detail.progress.cursor.上次水位 }} → 当前 {{ detail.progress.cursor.当前水位 }}</td>
              <td>累计 {{ detail.progress.cursor.增量条数 }} 条</td>
              <td>{{ detail.progress.cursor.采集时间 }}</td>
            </tr>
            <tr v-if="detail.progress?.latest_log">
              <td>最近批次日志</td>
              <td>{{ detail.progress.latest_log.关联源端 }}</td>
              <td>{{ detail.progress.latest_log.起始水位 }} → {{ detail.progress.latest_log.结束水位 }}</td>
              <td>{{ detail.progress.latest_log.轮次 }} #{{ detail.progress.latest_log.批次序号 }} · {{ detail.progress.latest_log.采集行数 }} 条</td>
              <td>{{ detail.progress.latest_log.结束时间 }}</td>
            </tr>
            <tr v-if="!detail.progress?.checkpoint">
              <td colspan="5" class="empty-state">尚未产生采集进度</td>
            </tr>
          </tbody>
        </table>
        <p v-if="consistencyNote" class="success-text">{{ consistencyNote }}</p>

        <h4>批次日志（历史保留当时结果，只追加不改写）</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>日志编号</th><th>轮次</th><th>批次</th><th>源端</th>
              <th>起始水位</th><th>结束水位</th><th>采集行数</th><th>状态</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="logRow in detail.logs" :key="logRow.日志编号">
              <td>{{ logRow.日志编号 }}</td>
              <td>{{ logRow.轮次 }}</td>
              <td>#{{ logRow.批次序号 }}</td>
              <td>{{ logRow.关联源端 }}</td>
              <td>{{ logRow.起始水位 }}</td>
              <td>{{ logRow.结束水位 }}</td>
              <td>{{ logRow.采集行数 }}</td>
              <td>{{ logRow.status }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, any>

const ENDPOINT = '/api/task'
const columns = ['任务编号', '任务名称', '关联源端', '采集频率', '增量字段', '起始水位', '批次大小', '执行窗口']
const actions = ['提交执行', '暂停任务', '恢复任务']

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const successMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const busy = ref('')
const detail = ref<Row | null>(null)
const resumeItems = ref<Row[]>([])

const stats = computed(() => [
  { label: '采集任务总数', value: total.value },
  { label: '待执行任务', value: rows.value.filter((r) => r.status === '待执行').length },
  { label: '执行中任务', value: rows.value.filter((r) => r.status === '执行中').length },
  { label: '断点待续做', value: resumeItems.value.length },
])

const consistencyNote = computed(() => {
  const d = detail.value
  if (!d?.progress?.checkpoint || !d.progress.latest_log) return ''
  const sameSource = d.progress.checkpoint.source === d.progress.latest_log.关联源端
  const sameWatermark = Number(d.progress.checkpoint.watermark) === Number(d.progress.latest_log.结束水位)
  return sameSource && sameWatermark
    ? '✓ 断点、游标与最近批次日志对应同一源端、同一批次，进度口径一致'
    : '× 断点与最近批次日志口径不一致，请检查回写链路'
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '采集任务登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) throw new Error('采集任务动作未生效，请稍后重试')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集任务操作失败'
  }
}

async function postAction(path: string, body: Record<string, unknown> = {}) {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  return response.json() as Promise<Row>
}

async function collectTask(row: Row) {
  errorMessage.value = ''
  successMessage.value = ''
  busy.value = row.任务编号
  try {
    const result = await postAction(`${ENDPOINT}/${row.id}/collect`, { values: {} })
    if (!result.ok) throw new Error(result.message)
    const e = result.entry
    successMessage.value = `${row.任务编号} ${e.round_no} 完成 ${e.batch_count} 个批次、${e.round_fetched} 条，断点水位 ${e.resume_watermark}${e.finished ? '（已追平）' : '，下一轮续做'}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '窗口采集失败'
  } finally {
    busy.value = ''
  }
}

async function resumeTask(taskNo: string) {
  errorMessage.value = ''
  successMessage.value = ''
  busy.value = taskNo
  const target = rows.value.find((r) => r.任务编号 === taskNo)
  try {
    const result = await postAction(`${ENDPOINT}/${target?.id ?? ''}/resume`, { values: {} })
    if (!result.ok) throw new Error(result.message)
    const e = result.entry
    successMessage.value = `${taskNo} 从断点 ${e.batches[0]?.start_watermark} 续做，本轮结束水位 ${e.resume_watermark}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '断点续做失败'
  } finally {
    busy.value = ''
  }
}

async function syncSchedule(row: Row) {
  errorMessage.value = ''
  successMessage.value = ''
  try {
    const result = await postAction(`${ENDPOINT}/${row.id}/sync-schedule`)
    if (!result.ok) throw new Error(result.message)
    successMessage.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '调度计划同步失败'
  }
}

async function openDetail(row: Row) {
  try {
    const response = await request(`${ENDPOINT}/${row.id}`)
    if (!response.ok) throw new Error('任务详情读取失败')
    detail.value = await response.json()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务详情读取失败'
  }
}

async function loadWorkbench() {
  try {
    const response = await request(`${ENDPOINT}/workbench/resume`)
    if (response.ok) {
      const payload = await response.json()
      resumeItems.value = payload.items ?? []
    }
  } catch {
    resumeItems.value = []
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) throw new Error('采集任务列表读取失败')
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadWorkbench()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集任务列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.resume-board {
  border: 1px solid #d7deea;
  border-radius: 10px;
  padding: 12px 16px;
  margin-bottom: 16px;
  background: #f7f9fc;
}

.resume-board h3 {
  margin: 0 0 8px;
  font-size: 15px;
}

.resume-tag {
  display: inline-block;
  margin-left: 6px;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 12px;
  color: #b45309;
  background: #fef3c7;
}

.progress-cell {
  cursor: pointer;
  color: #1d4ed8;
  white-space: nowrap;
}

.drawer-mask {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: flex;
  justify-content: flex-end;
  z-index: 50;
}

.drawer {
  width: min(880px, 92vw);
  height: 100%;
  overflow-y: auto;
  background: #fff;
  padding: 20px 24px;
}

.drawer-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px 24px;
  margin-bottom: 12px;
}

.success-text {
  color: #047857;
}

button:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}
</style>

/* ==========================================================================
   Multi-Agent Ecosystem · 裸机版 —— 前端主逻辑
   架构文档来源：第7章 前端UI完整规范
     7.1 左侧边栏（280px / 标题+新会话 / 搜索+会话列表 / 底部背景+设置）
     7.2 右侧主区域（顶部标签：对话、审批）
     7.3 对话页面（任务面板、消息区思考链、底部输入框、状态栏真实Token）
     7.4 首次启动强制流程（无配置文件 → 强制 API 配置向导 → 至少一个模型测试连通 → 进入工作台）

   硬性要求落实：
     · 状态栏所有数据来自后端 /api/task|/api/status 真实采集值，前端不做任何模拟
     · 高危审批由后端推送（waiting_approval），前端仅展示 + 提交，无法绕过
     · 所有 Agent、任务、审批数据均来自统一消息结构体与状态机
   ========================================================================== */
'use strict';

/* ----------------------------- 全局状态 ----------------------------- */
const state = {
  auth: { authenticated: false, localBypass: false, username: '' },
  config: null,
  /* 【需求点 二、工作区分组】左树数据：工作区(分组) -> 归属会话 */
  workspaces: [],
  collapsedWorkspaces: {},        // workspace_id -> true 表示已折叠
  currentWorkspaceId: null,       // 当前选中工作区（右侧上下文跟随）
  sessions: [],
  currentSessionId: null,
  tasks: [],
  messages: [],
  approvals: [],
  /* 【需求点 Bug8】高危审批改为消息区内联卡片：
     · inlineApprovals 存放"刚提交任务返回的 pending_approval"等临时注入的记录
     · inlineDecided 记录本页面会话中已裁决过的 approval_id（用于继续展示裁决结果卡片）
     · inlineApprovalBusy 记录正在提交的 approval_id
     · inlineApprovalErrors 记录提交失败的后端中文原因（常驻在卡片内，不随 toast 消失） */
  inlineApprovals: [],
  inlineDecided: {},
  inlineApprovalBusy: '',
  inlineApprovalErrors: {},
  /* 【新增】30 秒审批超时倒计时：approval_id → 截止时间戳（秒）；定时器句柄 */
  approvalDeadlines: {},
  approvalCountdownTimer: null,
  /* 【第三轮·Bug2】正在等待"审批提交后收敛"的审批单（approval_id → true），
     用于轮询出口判断与界面提示，避免停留在"提交中…" */
  pendingResumes: {},
  approvalScope: 'session',
  sortMode: 'recent',
  /* 【需求点 Bug10】视图选项：分组方式（workspace 按工作区 / flat 单列表）+ 排序方式（manual / recent）
     配置持久化到 localStorage，刷新页面保留用户偏好 */
  view: { group: 'workspace', sort: 'recent' },
  activeTab: 'chat',
  background: { type: 'preset', value: 'night' },
  wizard: { providers: [], results: {}, pendingKeys: {} },
  attachments: [],
  running: false,
  runningTaskId: null,
  pollTimer: null,
  detailTimer: null,
  stats: null,
  modelHint: '',
  loadedTaskIds: new Set(),
  /* 【需求点 Bug1】常驻错误/结果状态：不自动消失，修改输入后才清除 */
  alerts: { settings: null, wizard: null, login: null },
  providerNotices: {},            // provider -> {ok, html, code} 常驻结果
  /* 【需求点 二、Bug2】补位开关的本地乐观状态：
     仅在一次保存请求进行中临时使用；null 表示"以后端返回值为准" */
  pendingFallback: null,
  /* 【需求点 二、任务执行计时器】 */
  timer: {
    timerId: '', sessionId: '', taskId: '', title: '',
    status: 'idle',        // idle / running / paused / success / failed / cancelled
    source: 'idle',        // live（本次任务计时中）/ local（本次任务刚结束）/ history（历史会话回显）
    elapsed: 0,            // 秒
    startedAt: null, finishedAt: null,
    running: false,
  },
  timerInterval: null,     // 前端 1s 刷新句柄
  timerSyncCounter: 0,     // 每 N 次本地刷新与后端校准一次
  /* 【需求点 Bug9】底部统计栏 3 秒轮询句柄 */
  statsPollTimer: null,
  currentTaskStats: null,  // 当前 task 的后端真实统计快照
  cancelTaskId: null,      // 当前正在执行的任务（供「终止任务」使用）
  /* 【需求点 三、2】生态位补位记录 */
  ecosystemFallbacks: [],
  agentModels: [],
  /* ==================================================================
     【需求点 Bug2】协同思维链流式输出（SSE）状态
       chainEvents       当前任务的流式事件（按 seq 升序、已去重）
       chainTaskId       前端生成、POST 与 SSE 共用的 task_id
       chainSource       EventSource 句柄（同一时刻只允许一个）
       chainSawRealEvent 是否已收到真实链路事件（区分"通道未就绪"的空 done 帧）
       chainSeq          已接收的最大 seq（重连/降级轮询时 after=seq，避免重复渲染）
       chainPollTimer    SSE 不可用时的降级轮询句柄（/api/task/{id}/chain）
       chainCache        已结束任务的链路快照：task_id -> 事件数组
       chainRenderQueued 消息区重绘节流标记
     ================================================================== */
  chainEvents: [],
  chainTaskId: null,
  chainSource: null,
  chainSawRealEvent: false,
  chainSeq: 0,
  chainPollTimer: null,
  chainPollTaskId: null,
  chainCache: {},
  chainRenderQueued: false,
  liveChainCollapsed: false,      // 运行中思维链的折叠状态（重绘时保持用户选择）
  /* 【需求点 Bug1】欢迎页发起、后端尚未回传新 session_id 时置 true：
     先切到对话视图（让流式思维链可见），拿到 session_id 后由真实会话接管 */
  pendingNewSession: false,
  /* 【需求点 Bug3】任务面板：重绘节流标记 + 权威任务列表指纹 + 本地乐观任务行 */
  tasksRenderQueued: false,
  taskPanelSig: null,
  optimisticTaskId: null,
  /* 【需求点 Bug2 边界约束1】待审批阻塞态：
     任务处于 waiting_approval 时，普通输入提交被临时阻塞（提示先完成审批），
     审批按钮固定在输入框上方；审批裁决后自动退出阻塞态。 */
  approvalBlock: { active: false, approvalId: '', taskId: '', text: '' },
  /* 【新增需求2】本轮任务集合：任务栏只展示当前这一轮用户输入对应的任务，
     新消息提交时清空上一轮全部旧任务条目（不累积历史会话旧任务）。 */
  roundIds: [],
};

/* 第7.3 模型选择候选（与架构文档 1.3 一致）
   【需求点 BUG-NEW1 规则4】Kimi-Flash 已收拢进 Kimi 厂商分组，不再作为独立选项。 */
const MODEL_OPTIONS = [
  { id: 'auto', name: '自动编排（推荐）' },
  { id: 'deepseek', name: 'DeepSeek' },
  { id: 'qwen', name: '通义千问 Qwen' },
  { id: 'kimi', name: 'Kimi 月之暗面' },
  { id: 'glm', name: '智谱 GLM' },
];

const THINK_ICON = {
  think: '💭', write: '📝', edit: '✏️', exec: '⚙️', push: '📤', read: '📖',
};
const THINK_LABEL = {
  think: '思考', write: '写入', edit: '编辑', exec: '执行', push: '推送', read: '读取',
};
/* 任务状态机 ↔ 前端中文（第3章 3.4 五状态，不得增删） */
const STATUS_LABEL = {
  pending: '待执行', running: '进行中', waiting_approval: '等待审批',
  success: '已完成', failed: '失败',
};
const APPROVAL_STATUS_LABEL = {
  manual: '人工通过', auto: '自动放行', rejected: '已拒绝', pending: '等待审批',
  timeout: '审批超时（自动拒绝）',
};
const APPROVAL_STATUS_CLASS = {
  manual: 'status-manual', auto: 'status-auto', rejected: 'status-rejected', pending: 'status-pending',
  timeout: 'status-rejected',
};

/* ----------------------------- 工具函数 ----------------------------- */
const $ = (id) => document.getElementById(id);

/* ====================================================================
   【需求点 Bug3 · 全量重建】统一 Tooltip（唯一一套，删除全部历史 tooltip）
   --------------------------------------------------------------------
   历史问题：原生 title 提示 + 自定义 data-tip 浮层两套并存，
             部分元素同时带 title 与 data-tip 会出现"两个 tooltip"。
   本次做法（彻底重建）：
     · **只保留一套**：所有提示统一走 data-tip 属性 + 唯一浮层 #maeTip；
       全项目已无任何 title 属性（原生浏览器提示不再可能弹出，因此不会再有两个）。
     · 背景色适配当前网页背景：applyTipTheme() 依据用户选定的背景
       （预设背景取调色板、自定义图片取图像平均色并压暗）计算浮层底色，
       通过 CSS 变量 --tip-bg / --tip-border 注入，字体恒为白色（#fff）。
     · 浮层是 body 末尾单一 fixed 节点，不参与布局，不影响任何既有 UI。
   ==================================================================== */
const TIP_SHOW_DELAY_MS = 120;
const TIP_FALLBACK_BG = 'rgba(18,20,28,0.94)';
/* 预设背景 → 浮层底色（与网页背景同一色系，白字始终可读） */
const TIP_PRESET_BG = {
  night: 'rgba(30,35,54,0.96)',
  black: 'rgba(18,18,22,0.96)',
  stars: 'rgba(20,24,44,0.96)',
};
const TIP_THEME_CACHE_KEY = 'mae.tipTheme.v1';
let tipNode = null;
let tipTimer = null;
let tipOwner = null;
let tipImageProbe = null;      // 自定义背景取样用的 Image（缓存，避免重复解码）

function ensureTipNode() {
  if (tipNode && document.body.contains(tipNode)) return tipNode;
  tipNode = document.createElement('div');
  tipNode.id = 'maeTip';
  tipNode.setAttribute('role', 'tooltip');
  tipNode.setAttribute('aria-hidden', 'true');
  document.body.appendChild(tipNode);
  return tipNode;
}

/* --------------------------------------------------------------------
   1) 背景色适配：把当前网页背景色换算为浮层底色（白字始终可读）
   -------------------------------------------------------------------- */
function readTipThemeCache() {
  try {
    const raw = JSON.parse(localStorage.getItem(TIP_THEME_CACHE_KEY) || '{}');
    return (raw && typeof raw === 'object') ? raw : {};
  } catch (e) { return {}; }
}

function writeTipThemeCache(cache) {
  try { localStorage.setItem(TIP_THEME_CACHE_KEY, JSON.stringify(cache)); } catch (e) { /* 忽略 */ }
}

/* 把任意颜色压暗到"白字可读"的亮度区间，并返回 rgba 字符串 */
function darkenForWhiteText(r, g, b, alpha) {
  const lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
  const factor = lum > 0.5 ? (0.42 / lum) : 0.92;    // 越亮压得越狠，暗色仅轻微压暗
  const rr = Math.max(0, Math.min(255, Math.round(r * factor)));
  const gg = Math.max(0, Math.min(255, Math.round(g * factor)));
  const bb = Math.max(0, Math.min(255, Math.round(b * factor)));
  return `rgba(${rr},${gg},${bb},${alpha})`;
}

/* 自定义背景图片 → 取样平均色（跨域/读取失败一律降级到兜底色，绝不报错） */
function sampleImageAverageColor(url, done) {
  if (!url || typeof document.createElement !== 'function') { done(null); return; }
  try {
    const img = tipImageProbe || (tipImageProbe = new Image());
    img.onload = () => {
      try {
        const size = 24;
        const canvas = document.createElement('canvas');
        canvas.width = size; canvas.height = size;
        const context = canvas.getContext ? canvas.getContext('2d') : null;
        if (!context) { done(null); return; }
        context.drawImage(img, 0, 0, size, size);
        const data = context.getImageData(0, 0, size, size).data;
        let r = 0; let g = 0; let b = 0; let skip = 0;
        for (let i = 0; i < data.length; i += 4) {
          if (data[i + 3] < 16) { skip += 1; continue; }   // 透明像素不计入
          r += data[i]; g += data[i + 1]; b += data[i + 2];
        }
        const count = (data.length / 4) - skip;
        if (!count) { done(null); return; }
        done([Math.round(r / count), Math.round(g / count), Math.round(b / count)]);
      } catch (err) { done(null); }
    };
    img.onerror = () => done(null);
    img.src = url;
  } catch (e) { done(null); }
}

/* 应用浮层主题：把适配后的底色写入 CSS 变量（白字由 CSS 固定） */
function applyTipTheme() {
  const b = state.background || { type: 'preset', value: 'night' };
  const root = document.documentElement;
  if (b.type === 'custom' && b.value) {
    const cache = readTipThemeCache();
    if (cache[b.value]) {
      root.style.setProperty('--tip-bg', cache[b.value]);
      root.style.setProperty('--tip-border', 'rgba(255,255,255,0.22)');
      return;
    }
    root.style.setProperty('--tip-bg', TIP_FALLBACK_BG);
    root.style.setProperty('--tip-border', 'rgba(255,255,255,0.22)');
    sampleImageAverageColor(b.value, (rgb) => {
      if (!rgb) return;
      const color = darkenForWhiteText(rgb[0], rgb[1], rgb[2], 0.96);
      const cacheNow = readTipThemeCache();
      cacheNow[b.value] = color;
      writeTipThemeCache(cacheNow);
      if ((state.background || {}).value === b.value) {
        root.style.setProperty('--tip-bg', color);
      }
    });
    return;
  }
  const color = TIP_PRESET_BG[b.value] || TIP_PRESET_BG.night;
  root.style.setProperty('--tip-bg', color);
  root.style.setProperty('--tip-border', 'rgba(255,255,255,0.22)');
}

/* --------------------------------------------------------------------
   2) 取提示文本：只认 data-tip（唯一数据源）
   -------------------------------------------------------------------- */
function tipTextOf(el) {
  if (!el || !el.getAttribute) return '';
  const tip = el.getAttribute('data-tip');
  return tip ? String(tip) : '';
}

function showTip(el) {
  const text = tipTextOf(el);
  if (!text) return;
  const node = ensureTipNode();
  applyTipTheme();                 // 每次展示都按当前网页背景刷新底色
  tipOwner = el;
  node.textContent = text;

  const rect = el.getBoundingClientRect();
  node.style.visibility = 'hidden';
  node.classList.add('show');
  const tipRect = node.getBoundingClientRect();
  const gap = 9;
  /* 默认放在元素下方；下方空间不足且上方够用 → 翻到上方 */
  const below = window.innerHeight - rect.bottom;
  const placeTop = below < tipRect.height + gap && rect.top > tipRect.height + gap;
  node.setAttribute('data-placement', placeTop ? 'top' : 'bottom');
  let top = placeTop ? (rect.top - tipRect.height - gap) : (rect.bottom + gap);
  top = Math.max(6, Math.min(top, window.innerHeight - tipRect.height - 6));
  let left = rect.left + rect.width / 2 - tipRect.width / 2;
  left = Math.max(8, Math.min(left, window.innerWidth - tipRect.width - 8));
  node.style.top = `${Math.round(top)}px`;
  node.style.left = `${Math.round(left)}px`;
  node.style.visibility = 'visible';
  node.setAttribute('aria-hidden', 'false');
}

function hideTip() {
  if (tipTimer) { clearTimeout(tipTimer); tipTimer = null; }
  tipOwner = null;
  if (tipNode) {
    tipNode.classList.remove('show');
    tipNode.setAttribute('aria-hidden', 'true');
  }
}

function scheduleTip(el) {
  if (el === tipOwner) return;
  hideTip();
  tipTimer = setTimeout(() => { tipTimer = null; showTip(el); }, TIP_SHOW_DELAY_MS);
}

function initTooltips() {
  ensureTipNode();
  applyTipTheme();
  // 去掉捕获模式 true，使用冒泡；增加判断：如果当前已经是同一个tipOwner直接return，防止重复调度
  document.addEventListener('mouseover', (e) => {
  // 只取当前鼠标直接命中的元素，不要向上closest，避免嵌套data-tip同时触发
  const el = e.target.matches('[data-tip]') ? e.target : null;
  if (!el || el === tipOwner) return;
  scheduleTip(el);
}, false);


  document.addEventListener('mouseout', (e) => {
  const el = e.target.matches('[data-tip]') ? e.target : null;
    if (!el) return;
    const to = e.relatedTarget;
    if (to && el.contains(to)) return;
    hideTip();
  }, false);

  document.addEventListener('focusin', (e) => {
const el = e.target.matches('[data-tip]') ? e.target : null;

    if (el && el !== tipOwner) scheduleTip(el);
  }, false);

  document.addEventListener('focusout', hideTip, false);
  document.addEventListener('click', hideTip, false);
  window.addEventListener('scroll', hideTip);
  window.addEventListener('resize', hideTip);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hideTip(); });
}



function escapeHtml(str) {
  return String(str == null ? '' : str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function nowStr(ts) {
  const d = ts ? new Date(ts) : new Date();
  const p = (n) => String(n).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}

function relTime(seconds) {
  const s = Math.floor(seconds || 0);
  if (s < 60) return `${s}秒`;
  if (s < 3600) return `${Math.floor(s / 60)}分`;
  if (s < 86400) return `${Math.floor(s / 3600)}小时`;
  return `${Math.floor(s / 86400)}天`;
}

/* Markdown 轻量渲染：标题/加粗/列表/代码块/引用/段落 */
function formatText(text) {
  if (!text) return '';
  const codeBlocks = [];
  let src = String(text).replace(/```(\w*)\n?([\s\S]*?)```/g, (m, lang, code) => {
    codeBlocks.push(`<pre><code>${escapeHtml(code)}</code></pre>`);
    return `\u0000CB${codeBlocks.length - 1}\u0000`;
  });
  src = escapeHtml(src);
  src = src.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  src = src.replace(/`([^`]+?)`/g, '<code>$1</code>');

  const blocks = src.split(/\n{2,}/);
  const html = blocks.map((block) => {
    const cbMatch = block.match(/^\u0000CB(\d+)\u0000$/);
    if (cbMatch) return codeBlocks[Number(cbMatch[1])];
    if (/^###\s/.test(block)) return `<h4>${block.replace(/^###\s/, '')}</h4>`;
    if (/^##\s/.test(block)) return `<h4>${block.replace(/^##\s/, '')}</h4>`;
    if (/^#\s/.test(block)) return `<h4>${block.replace(/^#\s/, '')}</h4>`;
    if (/^&gt;\s?/.test(block)) {
      return `<blockquote>${block.replace(/^&gt;\s?/gm, '')}</blockquote>`;
    }
    if (/^\s*[-*]\s/.test(block)) {
      const items = block.split('\n').filter((l) => l.trim())
        .map((l) => `<li>${l.replace(/^\s*[-*]\s/, '')}</li>`).join('');
      return `<ul>${items}</ul>`;
    }
    if (/^\s*\d+[.)]\s/.test(block)) {
      const items = block.split('\n').filter((l) => l.trim())
        .map((l) => `<li>${l.replace(/^\s*\d+[.)]\s/, '')}</li>`).join('');
      return `<ul>${items}</ul>`;
    }
    return `<p>${block.replace(/\n/g, '<br>')}</p>`;
  }).join('');
  return html.replace(/\u0000CB(\d+)\u0000/g, (m, i) => codeBlocks[Number(i)]);
}


let toastTimer = null;
/* 瞬时轻提示（用于成功/中性反馈，2.6 秒自动消失） */
function toast(msg) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  requestAnimationFrame(() => t.classList.add('show'));
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    t.classList.remove('show');
    setTimeout(() => t.classList.add('hidden'), 200);
  }, 2600);
}

/* ====================================================================
   【需求点 Bug1】常驻错误提示组件
   · 最小展示时长 3000ms：到时间后仅在「用户未修改输入」时才允许自动收起
   · 更推荐的行为：常驻直到用户修改对应输入框（或手动关闭）
   · 完整渲染后端返回的错误文本（不截断），并提供关闭按钮（手动关闭属于用户显式操作）
   ==================================================================== */
const ALERT_ICON = { error: '⛔', warn: '⚠️', ok: '✅', info: 'ℹ️' };
const ALERT_MIN_VISIBLE_MS = 3000;   // 【需求点 Bug1】最小展示时长常量

function setAlert(scope, payload) {
  const prev = state.alerts[scope];
  if (prev && prev.timer) clearTimeout(prev.timer);
  const entry = payload
    ? { ...payload, scope, shownAt: Date.now(), timer: null, dirty: false }
    : null;
  state.alerts[scope] = entry;
  renderAlert(scope);

  // 最小展示时长保护：到期后仅在"用户尚未修改输入"时才自动收起
  if (entry) {
    entry.timer = setTimeout(() => {
      const current = state.alerts[scope];
      if (!current || current.shownAt !== entry.shownAt) return;
      if (current.dirty) return;              // 用户已修改输入 → 已由 input 事件清除
      // 错误级别保持常驻（避免一闪而过），仅 ok/info 自动收起
      if (current.level === 'ok' || current.level === 'info') {
        state.alerts[scope] = null;
        renderAlert(scope);
      }
    }, ALERT_MIN_VISIBLE_MS);
  }
}

function clearAlert(scope) {
  const current = state.alerts[scope];
  if (!current) return;
  // 【需求点 Bug1】最小展示时长：未满 3000ms 时先标记 dirty，由定时器到点后收起
  const elapsed = Date.now() - (current.shownAt || 0);
  if (elapsed < ALERT_MIN_VISIBLE_MS && current.level !== 'ok' && current.level !== 'info') {
    current.dirty = true;
    return;
  }
  if (current.timer) clearTimeout(current.timer);
  state.alerts[scope] = null;
  renderAlert(scope);
}

function clearProviderNotice(provider) {
  if (!state.providerNotices[provider]) return;
  delete state.providerNotices[provider];
  const el = $(`test-${provider}`);
  if (el) { el.className = 'test-result'; el.innerHTML = ''; }
}

function renderAlert(scope) {
  const el = $(`${scope}Alert`);
  if (!el) return;
  const a = state.alerts[scope];
  if (!a) { el.className = 'alert-banner hidden'; el.innerHTML = ''; return; }
  const level = a.level || 'error';
  el.className = 'alert-banner' + (level === 'error' ? '' : ' ' + level);
  el.innerHTML = `
    <span class="ab-icon">${ALERT_ICON[level] || '⛔'}</span>
    <div class="ab-content">
      <span class="ab-title">${escapeHtml(a.title || '操作失败')}${a.code ? `（${escapeHtml(a.code)}）` : ''}</span>
      <span class="ab-msg">${escapeHtml(a.message || '')}</span>
      ${a.hint ? `<span class="ab-hint">建议：${escapeHtml(a.hint)}</span>` : ''}
      ${a.raw ? `<span class="ab-raw">${escapeHtml(a.raw)}</span>` : ''}
    </div>
    <button class="ab-close" data-alert-close="${escapeHtml(scope)}" data-tip="关闭提示">✕</button>`;
}

/* 【需求点 三、2】生态位补位非阻断轻提示 */
let ecoTimer = null;
function showEcoNotice(fallbacks) {
  const list = fallbacks || [];
  if (!list.length) return;
  const first = list[0];
  $('ecoNoticeText').textContent = first.note
    || `当前缺失${first.original_model}模型，生态位补位，实际使用：${first.actual_model}`;
  const more = list.length > 1
    ? `本次任务共 ${list.length} 个 Agent 触发补位：` +
      list.map((f) => `${f.agent_role} → ${f.actual_model}`).join('；')
    : `Agent：${first.agent_role} · 原指定模型：${first.original_model} · 原始失败原因：${(first.reason || '').slice(0, 120)}`;
  $('ecoNoticeDetail').textContent = more;
  const box = $('ecoNotice');
  box.classList.remove('hidden');
  clearTimeout(ecoTimer);
  // 非阻断轻提示：不阻塞任何操作，16 秒后自动收起，也可手动关闭
  ecoTimer = setTimeout(hideEcoNotice, 16000);
}
function hideEcoNotice() {
  clearTimeout(ecoTimer);
  $('ecoNotice').classList.add('hidden');
}

/* 设置/向导弹窗中，把结构化错误渲染成完整卡片（不截断） */
function noticeHtml(data) {
  const ok = Boolean(data.ok);
  return `
    <span class="tr-title">${ok ? '✅' : '❌'} ${escapeHtml(data.message || (ok ? '连通正常' : '连通失败'))}</span>
    ${data.error_category || data.error_code
      ? `<span class="tr-hint">错误类别：${escapeHtml(data.error_category || '')}${data.error_code ? `（${escapeHtml(data.error_code)}）` : ''} · HTTP ${data.http_status || 0} · 耗时 ${data.elapsed_ms || 0}ms</span>`
      : `<span class="tr-hint">HTTP ${data.http_status || 0} · 耗时 ${data.elapsed_ms || 0}ms</span>`}
    ${data.hint ? `<span class="tr-hint">建议：${escapeHtml(data.hint)}</span>` : ''}
    ${data.raw_detail ? `<span class="tr-raw">${escapeHtml(data.raw_detail)}</span>` : ''}`;
}

function openModal(id) { $(id).classList.remove('hidden'); }
function closeModal(id) { $(id).classList.add('hidden'); }

/* ----------------------------- API 封装 ----------------------------- */
async function api(path, options = {}) {
  const opts = { credentials: 'same-origin', headers: {}, ...options };
  if (opts.body !== undefined && !(opts.body instanceof FormData)) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(opts.body);
  }
  let resp;
  try {
    resp = await fetch(path, opts);
  } catch (e) {
    throw new Error(`无法连接后端服务（${e.message}）`);
  }
  const text = await resp.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch (e) { data = { ok: false, message: text }; }
  if (!resp.ok) {
    const err = new Error(data.message || data.detail || `HTTP ${resp.status}`);
    err.status = resp.status;
    err.code = data.error;
    err.payload = data;
    throw err;
  }
  return data;
}

/* ====================================================================
   背景（第7.1 背景功能：预设背景 + 自定义图片上传）
   ==================================================================== */
function applyBackground() {
  const b = state.background || { type: 'preset', value: 'night' };
  document.body.className = '';
  document.body.style.backgroundImage = '';
  if (b.type === 'custom' && b.value) {
    document.body.style.backgroundImage = `url(${b.value})`;
  } else {
    document.body.className = 'bg-' + (b.value || 'night');
  }
  /* 【需求点 Bug3】背景变化 → 同步刷新 tooltip 底色（适配当前网页背景，白字不变） */
  try { applyTipTheme(); } catch (e) { /* tooltip 主题失败不影响背景切换 */ }
}

function renderBgOptions() {
  document.querySelectorAll('.bg-option[data-bg]').forEach((el) => {
    el.classList.toggle('selected',
      state.background.type === 'preset' && state.background.value === el.dataset.bg);
  });
  $('bgUploadOption').classList.toggle('selected', state.background.type === 'custom');
}

/* ====================================================================
   【需求点 Bug10】视图选项（分组方式 / 排序方式）
     · 分组方式：workspace = 按工作区分组（会话收纳在可折叠分组下）
                 flat      = 单列表（全部会话平铺展示，不分组）
     · 排序方式：manual    = 手动排序（沿用后端返回顺序 / updated_at 兜底）
                 recent    = 最近更新（按 updated_at 倒序）
     · 配置写入 localStorage，刷新页面后自动恢复
   ==================================================================== */
const VIEW_PREFS_KEY = 'mae.viewOptions.v1';
const VIEW_GROUP_MODES = ['workspace', 'flat'];
const VIEW_SORT_MODES = ['manual', 'recent'];

function loadViewPrefs() {
  try {
    const raw = JSON.parse(localStorage.getItem(VIEW_PREFS_KEY) || '{}');
    const group = VIEW_GROUP_MODES.includes(raw.group) ? raw.group : 'workspace';
    const sort = VIEW_SORT_MODES.includes(raw.sort) ? raw.sort : 'recent';
    state.view = { group, sort };
    state.sortMode = sort === 'recent' ? 'recent' : 'manual';
  } catch (e) {
    // localStorage 不可用（隐私模式等）→ 使用默认偏好，仅记录到控制台
    console.warn('[viewOptions] 读取视图偏好失败，使用默认值', e);
    state.view = { group: 'workspace', sort: 'recent' };
  }
  renderViewOptionsMenu();
}

function saveViewPrefs() {
  try {
    localStorage.setItem(VIEW_PREFS_KEY, JSON.stringify(state.view));
  } catch (e) {
    console.warn('[viewOptions] 保存视图偏好失败（不影响当前会话内使用）', e);
  }
}

/* 视图选项菜单：勾选态与 state.view 同步 */
function renderViewOptionsMenu() {
  const menu = $('viewOptionsMenu');
  if (!menu) return;
  menu.querySelectorAll('.vo-item').forEach((el) => {
    const isGroup = el.dataset.groupMode !== undefined;
    const value = isGroup ? el.dataset.groupMode : el.dataset.sortMode;
    const active = isGroup ? state.view.group === value : state.view.sort === value;
    el.classList.toggle('active', active);
    const check = el.querySelector('.vo-check');
    if (check) check.style.visibility = active ? 'visible' : 'hidden';
  });
  const btn = $('btnViewOptions');
  if (btn) {
    const g = state.view.group === 'flat' ? '单列表' : '按工作区';
    const s = state.view.sort === 'recent' ? '最近更新' : '手动排序';
  }
}

function toggleViewOptions(force) {
  const menu = $('viewOptionsMenu');
  if (!menu) return;
  const willShow = typeof force === 'boolean' ? force : menu.classList.contains('hidden');
  menu.classList.toggle('hidden', !willShow);
  if (willShow) renderViewOptionsMenu();
}

/* 切换选项 → 立即重渲染左侧列表（按新分组 / 新排序规则） */
function setViewOption(kind, value) {
  if (kind === 'group' && VIEW_GROUP_MODES.includes(value)) state.view.group = value;
  if (kind === 'sort' && VIEW_SORT_MODES.includes(value)) {
    state.view.sort = value;
    state.sortMode = value === 'recent' ? 'recent' : 'manual';
  }
  saveViewPrefs();
  renderViewOptionsMenu();
  renderWorkspaces();
  toggleViewOptions(false);
  toast(kind === 'group'
    ? `分组方式：${value === 'flat' ? '单列表' : '按工作区'}`
    : `排序方式：${value === 'recent' ? '最近更新' : '手动排序'}`);
}

/* 按当前视图偏好对会话排序 */
function sortSessionsForView(sessions) {
  const list = (sessions || []).slice();
  if (state.view.sort === 'recent') {
    list.sort((a, b) => (b.updated_at || 0) - (a.updated_at || 0));
  }
  // manual：保持后端返回顺序（后端已按插入顺序返回，作为手动排序基线）
  return list;
}

function sessionItemHtml(s, opts = {}) {
  return `
    <div class="session-item ${s.session_id === state.currentSessionId ? 'active' : ''}"
         data-session="${escapeHtml(s.session_id)}"
         data-tip="${escapeHtml((opts.workspaceName ? `工作区：${opts.workspaceName}\n` : '') + (s.dir || ''))}">
      <span class="session-icon">💬</span>
      <span class="session-title">${escapeHtml(s.title)}</span>
      ${opts.workspaceName ? `<span class="session-ws-tag">${escapeHtml(opts.workspaceName)}</span>` : ''}
      <span class="session-time">${escapeHtml(relTime((Date.now() / 1000) - (s.updated_at || 0)))}</span>
      <button class="ws-op" data-move-session="${escapeHtml(s.session_id)}" data-tip="移动到其他工作区">⇄</button>
    </div>`;
}

/* 单列表模式：全部会话平铺展示（不分组），并标注其所属工作区 */
function renderFlatSessionList(q) {
  const rows = [];
  (state.workspaces || []).forEach((ws) => {
    (ws.sessions || []).forEach((s) => rows.push({ ...s, _wsName: ws.name }));
  });
  let filtered = rows;
  if (q) {
    filtered = rows.filter((s) => (s.title || '').toLowerCase().includes(q)
      || (s._wsName || '').toLowerCase().includes(q));
  }
  const ordered = sortSessionsForView(filtered);
  if (!ordered.length) {
    return `<div class="ws-empty-global">${q ? '未找到匹配的会话' : '暂无会话，点击新会话来创建'}</div>`;
  }
  return `<div class="flat-list">${ordered.map((s) => sessionItemHtml(s, {
    workspaceName: s._wsName,
  })).join('')}</div>`;
}
function currentWorkspace() {
  return (state.workspaces || []).find((w) => w.workspace_id === state.currentWorkspaceId) || null;
}

/* ====================================================================
   侧边栏：会话列表渲染
     【需求点 Bug10】按视图选项渲染：
       · 分组方式 = workspace → 工作区（可折叠分组）→ 归属会话
       · 分组方式 = flat      → 全部会话平铺单列表
       · 排序方式 = recent    → 按最近更新倒序；manual → 保持后端顺序
   ==================================================================== */
function renderWorkspaces() {
  const wrap = $('workspaceList');
  const q = ($('searchInput').value || '').trim().toLowerCase();
  const groups = (state.workspaces || []).slice();

  // 【需求点 Bug10】单列表模式：全部会话平铺（不分组）
  if (state.view.group === 'flat') {
    if (!groups.length) {
      wrap.innerHTML = `<div class="ws-empty-global">暂无工作区<br>点击「＋ 新建工作区」创建工作区</div>`;
      return;
    }
    wrap.innerHTML = renderFlatSessionList(q);
    return;
  }

  if (!groups.length) {
    wrap.innerHTML = `<div class="ws-empty-global">暂无工作区<br>点击「＋ 新建工作区」创建工作区</div>`;
    return;
  }

  const html = groups.map((ws) => {
    let sessions = (ws.sessions || []).slice();
    if (q) {
      const hitWs = (ws.name || '').toLowerCase().includes(q);
      sessions = sessions.filter((s) => (s.title || '').toLowerCase().includes(q));
      if (!hitWs && !sessions.length) return '';
    }
    // 【需求点 Bug10】分组内的会话同样按排序方式排列
    sessions = sortSessionsForView(sessions);
    const collapsed = !!state.collapsedWorkspaces[ws.workspace_id];
    const active = ws.workspace_id === state.currentWorkspaceId;

    // 【需求点 二、2】无会话时展示空状态占位文字，不预生成任何会话条目
    const body = sessions.length
      ? sessions.map((s) => sessionItemHtml(s)).join('')
      : `<div class="ws-empty">暂无会话，点击新会话来创建</div>`;

    return `
      <div class="ws-group ${active ? 'active' : ''}" data-workspace="${escapeHtml(ws.workspace_id)}">
        <div class="ws-group-head" data-ws-head="${escapeHtml(ws.workspace_id)}">
          <span class="ws-caret ${collapsed ? 'collapsed' : ''}">▾</span>
          <span class="ws-icon">🗂</span>
          <span class="ws-name" data-tip="${escapeHtml(ws.name)}">${escapeHtml(ws.name)}</span>
          <span class="ws-count">${sessions.length}</span>
          <span class="ws-ops">
            <button class="ws-op" data-ws-new="${escapeHtml(ws.workspace_id)}" data-tip="在此工作区新建会话">＋</button>
            <button class="ws-op" data-ws-rename="${escapeHtml(ws.workspace_id)}" data-tip="重命名工作区">✎</button>
            <button class="ws-op danger" data-ws-delete="${escapeHtml(ws.workspace_id)}" data-tip="删除工作区">🗑</button>
          </span>
        </div>
        <div class="ws-sessions ${collapsed ? 'collapsed' : ''}">${body}</div>
      </div>`;
  }).join('');

  wrap.innerHTML = html || `<div class="ws-empty-global">未找到匹配的工作区或会话</div>`;
}

async function loadWorkspaces() {
  const q = ($('searchInput').value || '').trim();
  try {
    const url = '/api/workspace/list' + (q ? `?q=${encodeURIComponent(q)}` : '');
    const data = await api(url);
    state.workspaces = data.workspaces || [];
  } catch (e) {
    state.workspaces = [];
    if (e.status !== 401) toast(`工作区加载失败：${e.message}`);
  }
  // 保证始终有选中的工作区（默认第一个）
  if (!currentWorkspace() && state.workspaces.length) {
    state.currentWorkspaceId = state.workspaces[0].workspace_id;
  }
  renderWorkspaces();
  renderWsChip();
}

function renderWsChip() {
  const ws = currentWorkspace();
  // 【需求点 二、1】左上角工作区选择下拉按钮：显示当前激活工作区名称
  $('wsPickerName').textContent = ws ? ws.name : (state.workspaces.length ? '选择工作区' : '暂无工作区');
  // 兼容：旧版顶部 wsChip 已被左上角工作区选择器取代
  if ($('welcomeWorkspace')) $('welcomeWorkspace').textContent = ws ? ws.name : '（未选择工作区）';
  renderRootChip();
}

/* 【需求点 二、2 规则1/4】展示当前文件操作根目录（越权边界） */
function renderRootChip() {
  const el = $('rootChip');
  if (!el) return;
  const ws = currentWorkspace();
  const root = ws && ws.folder_path ? ws.folder_path : '';
  const isLocal = Boolean(ws && ws.kind === 'local_folder' && root);
  el.textContent = isLocal ? `🔒 ${root}` : '🔒 系统隔离目录';
  el.classList.toggle('system', !isLocal);
}

/* ====================================================================
   【需求点 二、1】工作区选择下拉菜单
   ==================================================================== */
function renderWsPickerMenu() {
  const menu = $('wsPickerMenu');
  if (menu.classList.contains('hidden')) return;
  const items = (state.workspaces || []).map((ws) => {
    const active = ws.workspace_id === state.currentWorkspaceId;
    const path = ws.folder_path || '';
    const missing = ws.kind === 'local_folder' && ws.folder_available === false;
    return `
      <div class="ws-picker-item ${active ? 'active' : ''}" data-pick-ws="${escapeHtml(ws.workspace_id)}">
        <span class="wpi-dot ${missing ? 'missing' : ''}"></span>
        <span class="wpi-name">${escapeHtml(ws.name)}
          ${path ? `<span class="wpi-path">${escapeHtml(path)}</span>` : ''}
        </span>
        <span class="wpi-meta">${ws.session_count} 会话</span>
      </div>`;
  }).join('') || '<div class="ws-empty">暂无工作区</div>';

  menu.innerHTML = `
    <div class="ws-picker-title">已保存的工作区（点击切换）</div>
    ${items}
    <div class="ws-picker-sep"></div>
    <div class="ws-picker-add" data-pick-add="1">＋ 添加工作区...</div>
    <div class="ws-picker-foot">
      添加后该文件夹即成为所有 Agent 的文件操作根目录；
      越界访问将被拒绝：越权访问禁止：只能操作当前工作目录内文件
    </div>`;
}

function toggleWsPicker(force) {
  const menu = $('wsPickerMenu');
  const willShow = typeof force === 'boolean' ? force : menu.classList.contains('hidden');
  menu.classList.toggle('hidden', !willShow);
  if (willShow) {
    renderWsPickerMenu();
    // 【需求点 一、Bug1】菜单为 position:fixed（脱离所有祖先裁剪），
    // 打开时按按钮实际位置计算坐标，并在滚动/缩放时保持跟随。
    positionWsPickerMenu();
    requestAnimationFrame(positionWsPickerMenu);
    window.addEventListener('resize', positionWsPickerMenu);
    window.addEventListener('scroll', positionWsPickerMenu, true);
  } else {
    window.removeEventListener('resize', positionWsPickerMenu);
    window.removeEventListener('scroll', positionWsPickerMenu, true);
  }
}

/* 【需求点 一、Bug1】把工作区下拉菜单定位到按钮正下方，并保证不出视口 */
function positionWsPickerMenu() {
  const menu = $('wsPickerMenu');
  const btn = $('btnWsPicker');
  if (!menu || !btn || menu.classList.contains('hidden')) return;

  const rect = btn.getBoundingClientRect();
  const gap = 8;
  const menuWidth = menu.offsetWidth || 320;

  // 水平：默认与按钮左对齐；若右侧越界则向左回收，始终不出视口
  let left = rect.left;
  const maxLeft = window.innerWidth - menuWidth - 12;
  if (left > maxLeft) left = Math.max(12, maxLeft);

  // 垂直：默认在按钮下方；若下方空间不足则向上翻转，避免被视口底部截断
  const menuHeight = Math.min(menu.offsetHeight || 0, window.innerHeight * 0.6);
  let top = rect.bottom + gap;
  if (top + menuHeight > window.innerHeight - 12) {
    const above = rect.top - gap - menuHeight;
    top = above >= 12 ? above : Math.max(12, window.innerHeight - menuHeight - 12);
  }

  menu.style.position = 'fixed';
  menu.style.left = `${Math.round(left)}px`;
  menu.style.top = `${Math.round(top)}px`;
  menu.style.minWidth = `${Math.round(rect.width)}px`;
}

/* ---------- 【需求点 二、1】添加工作区：系统原生文件夹选择 ---------- */
function folderPickerSupported() {
  return typeof window.showDirectoryPicker === 'function'
    && (window.isSecureContext !== false);
}

async function addWorkspaceFolder() {
  toggleWsPicker(false);
  if (folderPickerSupported()) {
    let handle = null;
    try {
      // 调用系统原生目录选择窗口（Chromium 系浏览器）
      handle = await window.showDirectoryPicker({ mode: 'readwrite', id: 'mae-workspace' });
    } catch (e) {
      if (e && e.name === 'AbortError') return;   // 用户取消
      // 【需求点 Bug3】目录读取/授权异常只在控制台记录，不弹任何浏览器原生弹窗
      console.warn('[workspace] 系统文件夹选择器异常（仅控制台记录，不弹窗）', {
        name: e && e.name, message: e && e.message,
      });
      handle = null;
    }
    if (handle && handle.name) {
      // 浏览器安全模型只暴露文件夹名称，交由后端在常用位置定位绝对路径
      try {
        const data = await api('/api/workspace/folder/probe', {
          method: 'POST', body: { folder_name: handle.name },
        });
        if (data.resolved && data.candidates.length === 1) {
          await registerFolderPath(data.candidates[0]);
          return;
        }
        if (data.candidates.length > 1) {
          openFolderCandidatePicker(handle.name, data.candidates);
          return;
        }
        // 未定位到 → 请用户粘贴完整路径
        openFolderPathDialog(handle.name,
          `已选择文件夹「${handle.name}」，但浏览器不暴露绝对路径，且未能在常用位置定位到它。请粘贴该文件夹的完整路径后确认。`);
        return;
      } catch (e) {
        // 【需求点 Bug3】探测失败 → 页面内引导手动填写路径（不弹原生弹窗）
        console.warn('[workspace] 文件夹路径探测失败（仅控制台记录，不弹窗）', e);
        openFolderPathDialog(handle.name,
          `定位失败：${friendlyWorkspaceError(e.payload || {}, e)}。请手动粘贴完整路径。`);
        return;
      }
    }
  }
  // 浏览器不支持原生目录选择（或非安全上下文）→ 手动填写路径
  openFolderPathDialog('',
    folderPickerSupported()
      ? '请输入要添加为工作区的文件夹完整路径。'
      : '当前浏览器/访问方式不支持系统文件夹选择器（需 Chromium 系浏览器 + http://127.0.0.1 或 HTTPS）。'
        + '请手动填写文件夹完整路径。');
}

function openFolderPathDialog(folderName, hint) {
  $('folderPathHint').textContent = hint || '请输入文件夹完整路径。';
  const input = $('folderPathInput');
  input.value = '';
  input.dataset.folderName = folderName || '';
  openModal('folderPathOverlay');
  setTimeout(() => input.focus(), 60);
}

function openFolderCandidatePicker(folderName, candidates) {
  $('folderPickHint').textContent =
    `「${folderName}」在常用位置检测到 ${candidates.length} 个同名文件夹，请选择要作为工作区的那一个：`;
  $('folderPickList').innerHTML = candidates.map((p) => `
    <div class="folder-item" data-pick-path="${escapeHtml(p)}">
      <span>📁</span><span class="folder-name">${escapeHtml(p)}</span>
    </div>`).join('');
  openModal('folderPickOverlay');
}

async function registerFolderPath(path) {
  try {
    const data = await api('/api/workspace/folder/add', { method: 'POST', body: { path } });
    if (!data.ok) {
      toast(data.message || '添加工作区失败');
      return;
    }
    state.currentWorkspaceId = data.workspace_id;
    closeModal('folderPathOverlay');
    closeModal('folderPickOverlay');
    /* 【需求点 Bug5 规则2】新建工作区完成之后自动创建一个空白新会话 */
    await autoCreateBlankSession(data.workspace_id);
    await loadWorkspaces();
    await refreshWorkspaceRoot();
    renderMainArea();
    await refreshStatus();
    setAlert('settings', {
      level: 'ok',
      title: '工作区已添加并切换',
      message: `「${data.folder.name}」已加入工作区列表并持久化保存。\n` +
               `文件操作根目录：${data.folder.path}\n` +
               '已自动创建一个空白新会话，可直接开始你的第一个任务。',
      hint: '所有 Agent 的文件读写/创建/修改/删除均限定在该目录内，越界访问会被后端拒绝。',
    });
    toast(`工作区已切换：${data.folder.name}（已自动创建空白会话）`);
  } catch (e) {
    /* 【需求点 Bug3】工作区目录读取异常：只在控制台记录 + 页面内友好提示，
       绝不弹出浏览器原生 alert/confirm */
    const payload = e.payload || {};
    console.warn('[workspace] 添加工作区失败（仅控制台记录，不弹原生弹窗）', {
      status: e.status, code: payload.error, message: payload.message || e.message,
    });
    setAlert('settings', {
      level: 'error',
      title: '添加工作区失败',
      code: payload.error || `HTTP_${e.status || 0}`,
      message: friendlyWorkspaceError(payload, e),
      hint: payload.hint || '请确认该目录存在、可写，且不是系统关键目录（如 C:\\Windows）。',
    });
    toast(`添加工作区失败：${payload.message || e.message}`);
  }
}

/* 【需求点 Bug3】工作区目录类异常 → 友好中文提示（区分不存在 / 无权限 / 非目录 / 系统目录） */
function friendlyWorkspaceError(payload, err) {
  const code = (payload && payload.error) || '';
  const map = {
    WORKSPACE_PATH_NOT_FOUND: '所选文件夹不存在或已被移动/删除，请重新选择。',
    WORKSPACE_PATH_NOT_DIR: '所选路径不是文件夹（可能是文件或快捷方式），请重新选择目录。',
    WORKSPACE_PATH_NOT_WRITABLE: '所选文件夹没有写入权限（可能为只读或被系统保护），请更换目录或以管理员身份运行。',
    WORKSPACE_PATH_FORBIDDEN: '所选目录属于系统关键目录，禁止作为 Agent 工作区，请更换为普通项目目录。',
    INVALID_WORKSPACE_PATH: '所选目录路径非法，请重新选择。',
    WORKSPACE_ACCESS_DENIED: '越权访问禁止：只能操作当前工作目录内文件。',
  };
  return map[code] || (payload && payload.message) || (err && err.message) || '未知错误';
}

/* 拉取当前工作区文件操作根目录（越权边界展示） */
async function refreshWorkspaceRoot() {
  if (!state.currentWorkspaceId) return;
  try {
    const data = await api(`/api/workspace/root?workspace_id=${encodeURIComponent(state.currentWorkspaceId)}`);
    state.workspaceRoot = data;
    if (data.folder_path && data.workspace_id) {
      const ws = (state.workspaces || []).find((w) => w.workspace_id === data.workspace_id);
      if (ws) {
        ws.folder_path = data.folder_path;
        ws.kind = data.kind;
      }
    }
  } catch (e) { /* 忽略：root 信息仅用于展示 */ }
  renderRootChip();
}

/* 会话扁平列表（供旧逻辑兼容：只取当前工作区下的会话） */
function flattenSessions() {
  const out = [];
  (state.workspaces || []).forEach((ws) => {
    (ws.sessions || []).forEach((s) => out.push({ ...s, workspace_id: ws.workspace_id }));
  });
  return out;
}

async function selectWorkspace(id) {
  state.currentWorkspaceId = id;
  // 切换工作区：右侧上下文跟随切换（未选中会话 -> 回到欢迎首页）
  const ws = (state.workspaces || []).find((w) => w.workspace_id === id);
  const belongs = ws && (ws.sessions || []).some((s) => s.session_id === state.currentSessionId);
  if (!belongs) {
    state.currentSessionId = null;
    state.tasks = [];
    state.messages = [];
  }
  /* 【需求点 Bug2】切换工作区 → 重置计时器状态（不残留上一个工作区的旧计时） */
  resetTimerState();
  /* 【需求点 Bug9】切换工作区 → 重置底部统计面板 */
  resetStatsPanel();
  renderWorkspaces();
  renderWsChip();
  if (state.currentSessionId) {
    await refreshSessionData();
  } else {
    renderMainArea();
  }
  await refreshStatus();
}

async function newSession(workspaceId) {
  const target = workspaceId || state.currentWorkspaceId;
  try {
    const data = await api('/api/session/create', {
      method: 'POST',
      body: { title: '', workspace_id: target || null },
    });
    state.currentSessionId = data.session.session_id;
    if (!state.currentWorkspaceId && data.session.workspace_id) {
      state.currentWorkspaceId = data.session.workspace_id;
    }
    state.tasks = []; state.messages = []; state.approvals = [];
    /* 【需求点 Bug8】新会话：清空内联审批卡片的本地注入与裁决记忆，避免串会话残留 */
    resetInlineApprovalTracking();
    state.loadedTaskIds = new Set();
    /* 【Bug3】新会话：清空"本轮任务"标记，避免沿用上一会话的 roundIds 过滤掉新会话任务 */
    state.roundIds = [];
    /* 【需求点 Bug2 规则1】切换会话 → 重置计时器状态 */
    resetTimerState();
    /* 【需求点 Bug9 规则3】切换会话 → 重置统计面板 */
    resetStatsPanel();
    await loadWorkspaces();
    await refreshSessionData();
    await refreshStatus();
    toast('已创建新会话（独立工作目录已初始化）');
  } catch (e) {
    toast(`新建会话失败：${e.message}`);
  }
}

async function selectSession(id) {
  if (state.currentSessionId === id) return;
  state.currentSessionId = id;
  /* 【需求点 Bug8】切换会话：仅保留当前会话的审批卡片（pending 记录由后端重新拉取） */
  resetInlineApprovalTracking();
  state.loadedTaskIds = new Set();
  /* 【Bug3】切换会话：清空上一会话的"本轮任务"标记，任务栏按新会话数据展示 */
  state.roundIds = [];
  /* 【Bug2 修复】切换会话：丢弃上一会话的本地占位行（乐观行 / plan 行） */
  dropLocalOnlyTaskRows();
  /* 【需求点 Bug2 规则1】切换会话 → 重置计时器状态（随后按新会话真实计时记录回显） */
  resetTimerState();
  /* 【需求点 Bug9 规则3】切换会话 → 重置统计面板并立即重新拉取 */
  resetStatsPanel();
  renderWorkspaces();
  await refreshSessionData();
  await refreshStatus();
}

/* ---------- 【需求点 二、3 / Bug5】工作区操作 ---------- */
/* 【需求点 Bug5 规则2】新建工作区完成之后自动创建一个空白新会话 */
async function autoCreateBlankSession(workspaceId) {
  try {
    const data = await api('/api/session/create', {
      method: 'POST', body: { title: '', workspace_id: workspaceId || null },
    });
    state.currentSessionId = data.session.session_id;
    state.tasks = []; state.messages = []; state.approvals = [];
    resetInlineApprovalTracking();
    state.loadedTaskIds = new Set();
    /* 【Bug3】新会话：清空"本轮任务"标记 */
    state.roundIds = [];
    /* 【需求点 Bug2】空会话没有任何计时记录 → 计时器回到「未开始任务」 */
    resetTimerState();
    /* 【需求点 Bug9】切换会话 → 重置底部统计面板 */
    resetStatsPanel();
    return data.session;
  } catch (e) {
    console.warn('[workspace] 自动创建空白会话失败（仅记录控制台，不弹窗）', e);
    state.currentSessionId = null;
    state.tasks = []; state.messages = [];
    return null;
  }
}

async function createWorkspace() {
  try {
    /* 【需求点 Bug5 规则1】优先走「系统文件夹选择弹窗」绑定本地目录；
       若浏览器不支持目录选择器，再由用户手动粘贴路径（不弹原生 alert） */
    if (folderPickerSupported()) {
      await addWorkspaceFolder();
      return;
    }
    const data = await api('/api/workspace/create', { method: 'POST', body: { name: '新工作区' } });
    state.currentWorkspaceId = data.workspace.workspace_id;
    state.collapsedWorkspaces[data.workspace.workspace_id] = false;
    /* 【需求点 Bug5 规则2】新建工作区完成后自动创建一个空白新会话 */
    await autoCreateBlankSession(data.workspace.workspace_id);
    await loadWorkspaces();
    renderMainArea();
    await refreshStatus();
    toast('已创建工作区，并自动创建一个空白新会话');
  } catch (e) {
    console.warn('[workspace] 新建工作区失败（仅记录控制台，不弹窗）', e);
    toast(`新建工作区失败：${e.message}`);
  }
}

function openWorkspaceRename(workspaceId) {
  const ws = (state.workspaces || []).find((w) => w.workspace_id === workspaceId);
  if (!ws) return;
  $('wsRenameInput').value = ws.name || '';
  $('wsRenameInput').dataset.workspaceId = workspaceId;
  openModal('wsRenameOverlay');
  setTimeout(() => $('wsRenameInput').focus(), 60);
}

async function confirmWorkspaceRename() {
  const workspaceId = $('wsRenameInput').dataset.workspaceId;
  const name = $('wsRenameInput').value.trim();
  if (!name) { toast('工作区名称不能为空'); return; }
  try {
    await api(`/api/workspace/rename?workspace_id=${encodeURIComponent(workspaceId)}`,
      { method: 'POST', body: { name } });
    closeModal('wsRenameOverlay');
    await loadWorkspaces();
    toast('工作区已重命名');
  } catch (e) {
    toast(`重命名失败：${e.message}`);
  }
}

function openWorkspaceDelete(workspaceId) {
  const ws = (state.workspaces || []).find((w) => w.workspace_id === workspaceId);
  if (!ws) return;
  const count = (ws.sessions || []).length;
  $('wsDeleteText').textContent =
    `工作区：${ws.name}\n会话数量：${count} 个\n\n` +
    (count
      ? `确认后将同时删除下列会话及其任务、消息、审批记录：\n` +
        (ws.sessions || []).map((s) => `· ${s.title}`).join('\n')
      : `该工作区下暂无会话。`);
  $('btnWsDeleteConfirm').dataset.workspaceId = workspaceId;
  openModal('wsDeleteOverlay');
}

async function confirmWorkspaceDelete() {
  const workspaceId = $('btnWsDeleteConfirm').dataset.workspaceId;
  try {
    const data = await api(`/api/workspace/delete?workspace_id=${encodeURIComponent(workspaceId)}`,
      { method: 'POST' });
    closeModal('wsDeleteOverlay');
    const removed = data.removed || {};
    // 当前工作区/会话被删除时回到欢迎首页
    if (state.currentWorkspaceId === workspaceId) {
      state.currentWorkspaceId = null;
      state.currentSessionId = null;
      state.tasks = []; state.messages = [];
    }
    await loadWorkspaces();
    renderMainArea();
    toast(`工作区已删除：清理会话 ${removed.sessions || 0} 个、任务 ${removed.tasks || 0} 个`);
  } catch (e) {
    toast(`删除失败：${e.message}`);
  }
}

/* ---------- 【需求点 二、3】会话移动归属 ---------- */
function openSessionMove(sessionId) {
  const session = flattenSessions().find((s) => s.session_id === sessionId);
  $('sessionMoveText').textContent = session
    ? `会话「${session.title}」当前归属：${(currentWorkspace() || {}).name || '—'}；请选择目标工作区：`
    : '请选择目标工作区：';
  $('sessionMoveList').innerHTML = (state.workspaces || []).map((ws) => {
    const isCurrent = ws.workspace_id === (session ? session.workspace_id : null);
    return `<div class="ws-move-item ${isCurrent ? 'current' : ''}"
                 data-move-to="${escapeHtml(ws.workspace_id)}"
                 data-move-session-id="${escapeHtml(sessionId)}">
      <span>🗂 ${escapeHtml(ws.name)}</span>
      <span style="color:var(--text-faint);font-size:11px;">
        ${(ws.sessions || []).length} 个会话${isCurrent ? ' · 当前归属' : ''}
      </span>
    </div>`;
  }).join('') || '<div class="ws-empty">暂无其他工作区，请先新建工作区</div>';
  openModal('sessionMoveOverlay');
}

async function confirmSessionMove(sessionId, workspaceId) {
  try {
    await api('/api/session/move', {
      method: 'POST', body: { session_id: sessionId, workspace_id: workspaceId },
    });
    closeModal('sessionMoveOverlay');
    await loadWorkspaces();
    toast('会话归属已移动');
  } catch (e) {
    toast(`移动失败：${e.message}`);
  }
}

async function refreshSessionData() {
  const sid = state.currentSessionId;
  if (!sid) {
    // 【需求点 二、1】未选中任何会话 → 展示欢迎首页，不渲染空白聊天窗口
    /* 【需求点 Bug1/Bug2】欢迎页发起、新会话 ID 尚未回传时（任务正在跑）不清空消息区，
       否则会把刚渲染的"用户消息 + 流式思维链"抹掉 */
    if (!state.running) {
      state.tasks = []; state.messages = []; state.approvals = [];
      resetInlineApprovalTracking();   /* 【需求点 Bug8】无会话时不残留任何审批卡片 */
    }
    renderMainArea();
    return;
  }
  try {
    const data = await api(`/api/session/${encodeURIComponent(sid)}`);
    /* 【Bug3 · 修复】任务栏只展示"本轮用户输入"对应的任务集合：
       历史缺陷：这里曾把会话全部历史任务无差别写入 state.tasks，任务完成后
       refreshSessionData 被再次调用，任务栏就累积出所有旧任务条目。
       现在与 refreshTaskPanel 同源：按 roundIds（本轮根任务 + 其子任务）过滤；
       roundIds 为空（打开历史会话）时不过滤，展示该会话最近一轮任务。
       【Bug2 修复】改用合并逻辑：审批恢复轮询期间 plan 占位行不丢失，
       权威子任务行落库后按同标题自然取代占位行。 */
    state.tasks = mergeAuthoritativeTasks(data.tasks || []);
    state.messages = data.messages || [];
    state.stats = data.tokens || null;
    if (data.session && data.session.workspace_id) {
      // 保证左树高亮与该会话归属一致
      state.currentWorkspaceId = data.session.workspace_id;
    }
    (state.tasks || []).forEach((t) => state.loadedTaskIds.add(t.task_id));
  } catch (e) {
    if (e.status === 404) {
      // 会话已被删除，回到欢迎首页
      state.currentSessionId = null;
      state.tasks = []; state.messages = [];
      await loadWorkspaces();
      renderMainArea();
      return;
    }
    toast(`会话数据加载失败：${e.message}`);
  }
  await loadApprovals();
  /* 【需求点 Bug2 边界约束1】按后端权威审批记录同步"待审批阻塞态"：
     刷新页面 / 切换会话 / 审批后刷新，都能正确恢复或解除输入拦截 */
  if ((state.approvals || []).some((r) => r && r.state === 'pending')) {
    syncApprovalBlockFromRecords();
  } else {
    exitApprovalBlock();
  }
  // 【需求点 二、3】历史会话打开 → 回显该会话上一次任务耗时（无记录则「未开始任务」）
  await loadSessionTimer();
  /* 【需求点 Bug2】刷新页面 / 切换会话 → 用 /api/task/{id}/chain 补齐最近一次任务的
     流式思维链（失败静默降级；无链路时消息区回退消息自带 think_steps，行为不退化） */
  await loadLatestChainSnapshot();
  renderAll();
}

/* ====================================================================
   【需求点 二、1】主面板区域切换：欢迎首页 / 对话页面
   ==================================================================== */
function renderMainArea() {
  /* 【需求点 Bug1/Bug2】欢迎页发起任务、后端尚未回传新 session_id 期间（pendingNewSession），
     先行切到对话视图，使用户能立即看到流式思维链；拿到 session_id 后由真实会话接管 */
  let hasSession = Boolean(state.currentSessionId) || Boolean(state.pendingNewSession);
  /* 【需求点 Bug5 规则3】当前会话没有任何消息内容 → 渲染初始欢迎首页界面；
     用户输入对话发送后（messages 非空 / pendingNewSession）→ 自动切换到普通对话视图 */
  const sessionHasContent = (state.messages || []).length > 0
    || (state.tasks || []).length > 0
    || Boolean(state.running);
  if (hasSession && !sessionHasContent && !state.pendingNewSession) {
    hasSession = false;
  }
  $('welcomePage').classList.toggle('hidden', hasSession);
  $('chatView').classList.toggle('hidden', !hasSession);
  $('inputArea').classList.toggle('hidden', !hasSession);
  if (!hasSession) {
    const ws = currentWorkspace();
    $('welcomeWorkspace').textContent = ws ? ws.name : '（未选择工作区）';
    /* 【需求点 Bug5 规则3】初始界面标题固定为「开始你的第一个任务」 */
    $('welcomeTitle').textContent = '开始你的第一个任务';
    renderStatusBar();
  }
  renderWsChip();
  renderWorkspaces();
}

/* ====================================================================
   【需求点 Bug2】协同思维链流式输出（SSE + 降级快照轮询）
   --------------------------------------------------------------------
   后端契约（后端已实现，前端只消费、不改动）：
     · POST /api/session/chat 是**同步阻塞**的：必须先用同一个 task_id 把
       SSE 通道订阅起来，再发 POST，否则拿不到执行期间的实时思维链；
     · GET /api/task/{task_id}/stream 为**命名事件**（event: xxx），
       必须逐个 addEventListener，依赖 es.onmessage 收不到任何数据；
     · GET /api/task/{task_id}/chain 是同一份事件的快照，用于
       EventSource 不可用/出错时的降级，以及刷新页面后补齐历史思维链。
   ==================================================================== */
/* 事件类型 → 思维链步骤类型（复用既有 renderThinking 的视觉与折叠交互）
   注意：这张表同时用于生成 SSE 监听列表，漏一项就会整类事件收不到 */
const CHAIN_EVENT_STEP = {
  task_created: 'think',   // 任务创建（含工作区根目录）
  plan: 'think',           // 调度规划Agent 完成拆解
  dispatch: 'push',        // 任务流转：谁分派给谁 + 双方模型（核心步骤）
  agent_step: 'think',     // Agent 思考步骤
  tool_call: 'exec',       // 工具调用（写入/编辑/执行，按 detail.step_type 细化）
  subtask_done: 'push',    // 子任务回执
  /* 【BUG-C】链路阻断与失败相关事件 */
  subtask_failed: 'exec',  // 子任务达到重试上限失败（红色错误标识）
  subtask_blocked: 'exec', // 下游子任务被链路阻断，未下发执行
  chain_blocked: 'exec',   // 整条链路阻断：跳过评估校验 / 交互交付
  fault_report: 'exec',    // 调度规划Agent 输出的故障诊断
  approval: 'exec',        // 高危操作待人工审批（历史事件名，保持兼容）
  /* 【需求点 Bug2】审批中断 / 恢复相关事件 */
  approval_request: 'exec',   // 高危操作待审批（审批按钮渲染数据源）
  approval_result: 'think',   // 审批结果回灌（执行一次 / 拒绝）
  /* 【新增】30 秒审批超时：后端看门狗自动判定超时（等价 rejected）后推送 */
  approval_timeout: 'exec',
  stream_paused: 'think',     // 业务循环中断：SSE 流式输出已停止
  stream_resumed: 'think',    // 审批结果已回灌，业务循环从快照断点继续
  captain_done: 'push',       // 队长判定用户任务是否全部完成
  task_status: 'think',    // 状态机流转
  done: 'push',            // 本次链路终态
  model_call: 'think',     // 模型调用（后端保留类型，前端兼容渲染）
  fallback: 'exec',        // 生态位补位（后端保留类型，前端兼容渲染）
};
/* 需要显式订阅的事件名：少订阅一个就收不到该类事件 */
const CHAIN_EVENT_TYPES = Object.keys(CHAIN_EVENT_STEP);

/* 【BUG-C 2】判定单条思维链步骤是否为错误（红色标识）。
   后端已给出 level / is_error，这里再按文本兜底，保证历史数据也能正确着色 */
function isErrorStep(step) {
  if (!step) return false;
  if (step.is_error === true) return true;
  if (String(step.level || '').toLowerCase() === 'error') return true;
  const text = String(step.text || '');
  return text.startsWith('发生错误：') || text.startsWith('错误：')
    || text.includes('重试达到上限') || text.includes('链路阻断')
    || text.includes('不存在 / 无读取权限');
}

/* 【需求点 Bug2】客户端生成任务 ID：POST 的 task_id 与 SSE 订阅完全一致；
   形如 tsk_<base36时间>_<随机>，匹配后端 ^[A-Za-z0-9_-]{8,64}$ */
function genClientTaskId() {
  return 'tsk_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

/* 【需求点 Bug2】单条流式事件 → 思维链步骤。
   step.text 直接使用事件的 text（后端已是中文可读描述且内嵌模型名）
   【BUG-C 2/5】同时透传 level / is_error，供 renderThinking 渲染红色错误标识 */
function chainEventToStep(evt) {
  const type = (evt && evt.event) || 'agent_step';
  const detail = (evt && evt.detail) || {};
  let stepType = CHAIN_EVENT_STEP[type] || 'think';
  if (type === 'tool_call') {
    const st = String(detail.step_type || '');
    stepType = ['write', 'edit', 'exec', 'push', 'read'].includes(st) ? st : 'exec';
  }
  const level = String(detail.level || evt.level || '').toLowerCase();
  const isError = level === 'error'
    || ['subtask_failed', 'subtask_blocked', 'chain_blocked', 'fault_report'].includes(type)
    || String(evt.status || '') === 'failed' && ['subtask_failed', 'subtask_blocked'].includes(type);
  return {
    index: Number(evt.seq || 0),
    type: stepType,
    text: evt.text || evt.title || '',
    agent: evt.agent_role || evt.to_agent || evt.from_agent || '',
    model: evt.model_label || '',
    level: level || (isError ? 'error' : 'info'),
    is_error: Boolean(isError),
    /* 【BUG-C 4】故障诊断原文（fault_report 事件携带），供消息区完整展示 */
    fault_report: detail.fault_report || (type === 'fault_report' ? detail : null),
  };
}

/* 【需求点 Bug2】事件入列：按 seq 去重，保证断线重连 / 降级轮询不出现重复步骤 */
function pushChainEvent(evt, taskId) {
  if (!evt || state.chainTaskId !== taskId) return false;
  const seq = Number(evt.seq || 0);
  if (seq && seq <= (state.chainSeq || 0)) return false;
  state.chainEvents.push(evt);
  if (seq) state.chainSeq = seq;
  state.chainSawRealEvent = true;
  return true;
}

/* 【需求点 Bug2/Bug3】事件 → 任务状态机五状态（无状态含义时返回空串） */
function chainEventStatus(evt) {
  const type = (evt && evt.event) || '';
  const s = (evt && evt.status) || '';
  const five = ['pending', 'running', 'waiting_approval', 'success', 'failed'];
  if (type === 'approval' || type === 'approval_request' || type === 'stream_paused') {
    return 'waiting_approval';
  }
  /* 【新增】30 秒审批超时 = 等价 rejected：任务侧收敛为 failed（前端不会误显示"等待审批"） */
  if (type === 'approval_timeout') return 'failed';
  /* 【新增】审批结果回灌 → 业务循环从快照断点继续：任务面板立即回到 running，
     否则任务行会一直停在"等待审批"直到下一次轮询纠正 */
  if (type === 'approval_result' || type === 'stream_resumed') return 'running';
  if (type === 'done') return s === 'failed' ? 'failed' : (s === 'success' ? 'success' : '');
  if (type === 'task_status') return five.includes(s) ? s : 'running';
  if (type === 'dispatch' || type === 'subtask_done') {
    return s === 'waiting_approval' ? 'waiting_approval' : 'running';
  }
  return '';
}

/* 【需求点 Bug3】把状态写进任务面板对应行（先按 task_id 匹配，再退回本地乐观行） */
function applyTaskStatus(taskId, status) {
  const rows = state.tasks || [];
  const row = rows.find((t) => t.task_id === taskId) || rows.find((t) => t.__optimistic);
  if (!row || row.status === status) return;
  row.status = status;
  state.taskPanelSig = null;
  queueTasksRender();
}

/* 【需求点 Bug3】任务面板重绘节流：同一帧内的多条事件只重绘一次 */
function queueTasksRender() {
  if (state.tasksRenderQueued) return;
  state.tasksRenderQueued = true;
  requestAnimationFrame(() => {
    state.tasksRenderQueued = false;
    renderTasks();
  });
}

/* 【需求点 Bug2】消息区重绘节流：新事件到达时合并成一帧重绘（并自动滚到底部） */
function scheduleChainRender() {
  if (state.chainRenderQueued) return;
  state.chainRenderQueued = true;
  requestAnimationFrame(() => {
    state.chainRenderQueued = false;
    if (!state.running) return;
    renderMessages();
  });
}

/* 【需求点 Bug2】收到一条链路事件：入列 → 同步任务面板 → 重绘消息区 */
function handleChainEvent(evt, taskId) {
  if (!pushChainEvent(evt, taskId)) return;
  /* 【需求点 Bug3】流式事件实时推进任务面板状态（乐观行立即变化，无需手动刷新） */
  const next = chainEventStatus(evt);
  if (next) applyTaskStatus(taskId, next);
  /* 【Bug2 修复】拆解完成的第一时间：plan 事件 → 任务栏立刻渲染全部子任务；
     dispatch / subtask_done 事件 → 占位行实时推进（认领权威任务号 + 状态收敛） */
  const type = (evt && evt.event) || '';
  if (type === 'plan') applyPlanSubtasks(evt, taskId);
  if (type === 'dispatch' || type === 'subtask_done') applySubtaskRowEvent(evt, taskId);
  if ((type === 'done' || type === 'task_status' || type === 'approval_timeout')
      && ['success', 'failed'].includes(String((evt && evt.status) || ''))) {
    settlePlanRows(String(evt.status));
  }
  /* 【需求点 Bug2】审批中断事件（approval_request / approval / stream_paused）到达
     → 立刻进入"待审批阻塞态"（输入框上方出现审批按钮、普通提交被拦截），
       并向后端拉取权威审批记录渲染【✅ 执行一次】【❌ 拒绝】按钮。 */
  if (type === 'approval_request' || type === 'approval' || type === 'stream_paused') {
    enterApprovalBlock(evt);
    syncPendingApprovals().catch(() => { /* 拉取失败不阻断流式渲染 */ });
  }
  if (type === 'approval_result') {
    /* 审批结果已回灌：退出阻塞态，队长业务循环继续跑 */
    exitApprovalBlock();
  }
  if (type === 'approval_timeout') {
    /* 【新增】30 秒审批超时（等价 rejected）：退出阻塞态 + 刷新权威审批状态；
       注意只追加提示，绝不覆盖/清空已有聊天内容（硬性约束1/3）。 */
    const detail = (evt && evt.detail) || {};
    if (detail.approval_id) clearApprovalDeadline(detail.approval_id);
    exitApprovalBlock();
    toast('审批已超时（30 秒无操作），后端自动按「拒绝」处理：高危操作未执行');
    syncPendingApprovals().catch(() => { /* 静默 */ });
  }
  scheduleChainRender();
}

/* 【需求点 Bug2】done：链路终态；也可能是"通道尚未建立/已被回收"的空帧 */
function handleChainDone(payload, taskId) {
  const status = (payload && payload.status) || '';
  /* 通道未就绪（POST 仍在途中，或已被回收）→ 不当作任务结束：
     改用 /chain 快照轮询兜底，等通道真正建立后继续取事件 */
  if (status === 'closed' || !payload || !payload.seq) {
    if (state.running && state.chainTaskId === taskId) startChainPolling(taskId);
    return;
  }
  handleChainEvent(payload, taskId);
  /* 真实终态到达 → 关闭实时订阅（权威结果由 afterTask 用会话接口渲染） */
  stopChainSource();
}

/* 【需求点 Bug2】只关闭实时订阅句柄（保留链路数据与 chainTaskId 供展示） */
function stopChainSource() {
  if (state.chainSource) {
    try { state.chainSource.close(); } catch (e) { /* 忽略 */ }
    state.chainSource = null;
  }
  stopChainPolling();
}

/* 【需求点 Bug2】任务收口：关闭实时订阅（链路事件保留给消息区展示） */
function closeChainStream() {
  stopChainSource();
}

/* 【需求点 Bug2】打开本任务的 SSE 通道（必须在 POST 之前调用） */
function openChainStream(taskId) {
  closeChainStream();
  if (!taskId) return;
  state.chainTaskId = taskId;
  if (typeof window.EventSource !== 'function') {
    /* 浏览器不支持 SSE → 降级为 /chain 快照轮询（静默，不影响任务本身） */
    startChainPolling(taskId);
    return;
  }
  let es = null;
  try {
    /* 显式使用 window.EventSource（与上面的能力探测同一引用，避免环境差异） */
    const ES = window.EventSource;
    es = new ES(
      '/api/task/' + encodeURIComponent(taskId) + '/stream?after=' + (state.chainSeq || 0));
  } catch (e) {
    startChainPolling(taskId);
    return;
  }
  state.chainSource = es;
  /* 命名事件必须逐个订阅（es.onmessage 不会触发） */
  CHAIN_EVENT_TYPES.forEach((type) => {
    es.addEventListener(type, (e) => {
      if (state.chainTaskId !== taskId) return;
      let payload = {};
      try { payload = JSON.parse(e.data || '{}'); } catch (err) { payload = {}; }
      if (type === 'done') handleChainDone(payload, taskId);
      else handleChainEvent(payload, taskId);
    });
  });
  /* 【需求点 Bug2】连接异常 → 关闭并优雅降级到 /chain 轮询兜底 */
  es.onerror = () => {
    if (state.chainSource === es) state.chainSource = null;
    try { es.close(); } catch (err) { /* 忽略 */ }
    if (state.running && state.chainTaskId === taskId) startChainPolling(taskId);
  };
}

/* 【需求点 Bug2】降级通道：轮询 /api/task/{id}/chain?after=seq 补齐链路事件 */
async function pollChainOnce(taskId) {
  try {
    const data = await api('/api/task/' + encodeURIComponent(taskId)
      + '/chain?after=' + (state.chainSeq || 0));
    (data.events || []).forEach((evt) => {
      if (evt.event === 'done') handleChainDone(evt, taskId);
      else handleChainEvent(evt, taskId);
    });
  } catch (e) { /* 静默降级：下一轮再试 */ }
}

function startChainPolling(taskId) {
  if (!taskId) return;
  if (state.chainPollTimer && state.chainPollTaskId === taskId) return;   // 已在轮询
  stopChainPolling();
  state.chainPollTaskId = taskId;
  state.chainPollTimer = setInterval(() => {
    if (!state.running || state.chainTaskId !== taskId) { stopChainPolling(); return; }
    pollChainOnce(taskId);
  }, 1500);
  pollChainOnce(taskId);   // 立即拉一次，避免白等一个周期
}

function stopChainPolling() {
  if (state.chainPollTimer) { clearInterval(state.chainPollTimer); state.chainPollTimer = null; }
  state.chainPollTaskId = null;
}

/* 【需求点 Bug2】读取某任务的链路快照（刷新页面 / 切换会话 / 任务收口时补齐历史思维链）。
   force=true 时以后端快照为准（比本地更完整）；失败静默降级到消息自带 think_steps */
async function loadChainSnapshot(taskId, force) {
  if (!taskId) return null;
  if (state.chainCache[taskId] && !force) return state.chainCache[taskId];
  try {
    const data = await api('/api/task/' + encodeURIComponent(taskId) + '/chain?after=0');
    const events = (data && data.events) || [];
    const local = state.chainCache[taskId];
    if (events.length && (!local || force || events.length > local.length)) {
      state.chainCache[taskId] = events;
    }
  } catch (e) { /* 静默降级：无快照时回退消息自带 think_steps（不阻断渲染） */ }
  return state.chainCache[taskId] || null;
}

/* 【需求点 Bug2】刷新 / 切换会话：为最近一次任务补齐链路快照（失败静默） */
async function loadLatestChainSnapshot() {
  const roots = (state.tasks || []).filter((t) => !t.parent_task_id && !t.__optimistic);
  const target = roots.length ? roots[roots.length - 1] : null;   // 任务列表按 created_at 升序
  if (!target) return;
  await loadChainSnapshot(target.task_id, false);
}

/* 【需求点 Bug2】取某任务可展示的思维链步骤：
   优先链路快照（含分派/工具调用等完整事件），其次回退消息自带 think_steps */
function stepsForTask(taskId, msg) {
  const cached = taskId ? state.chainCache[taskId] : null;
  if (cached && cached.length) return cached.map(chainEventToStep);
  const fromMsg = (msg && (msg.think_steps
    || (msg.metadata && msg.metadata.think_steps))) || null;
  return fromMsg && fromMsg.length ? fromMsg : [];
}

/* ====================================================================
   【新增需求2】任务栏只展示"当前这一轮用户输入"对应的任务集合
   --------------------------------------------------------------------
   业务规则：
     · 用户提交**一条全新的消息** → 清空上一轮全部旧任务条目；
     · 队长 Agent 解析本次输入、拆出任务列表 → 渲染到任务栏；
     · 任务栏不累积历史会话旧任务（历史任务仍在后端数据库与会话回放里，
       只是不在任务栏里堆积）。
   实现：用 roundIds（本轮根任务号集合，含本地乐观行）过滤任务列表，
        新消息提交时先清空 roundIds 与面板（clearTaskRoundForNewInput）。
   ==================================================================== */
function beginTaskRound(taskId) {
  state.roundIds = taskId ? [String(taskId)] : [];
  state.tasks = [];
  state.taskPanelSig = null;
  renderTasks();
}

function clearTaskRoundForNewInput(taskId) {
  state.roundIds = taskId ? [String(taskId)] : [];
  state.tasks = [];
  state.taskPanelSig = null;
}

/* 本轮任务集合：根任务本身 + 其后端子任务（parent_task_id 命中本轮根任务） */
function tasksOfCurrentRound(list) {
  const ids = new Set((state.roundIds || []).map(String));
  if (!ids.size) return list || [];
  return (list || []).filter((t) => {
    if (t.__optimistic && ids.has(String(t.task_id))) return true;
    if (ids.has(String(t.task_id))) return true;
    return Boolean(t.parent_task_id) && ids.has(String(t.parent_task_id));
  });
}

/* 【Bug2 修复】后端权威任务行与本地行（乐观行 / plan 占位行）合并：
   · 尚未落库的本地行保留（权威列表还没有它们）；
   · 占位行与本轮同标题的权威行去重（权威行带真实 task_id / 状态，取代占位行）。 */
function mergeAuthoritativeTasks(list) {
  const rows = list || [];
  const ids = new Set(rows.map((t) => t.task_id));
  const keep = (state.tasks || []).filter(
    (t) => (t.__optimistic || t.__plan) && !ids.has(t.task_id));
  const merged = tasksOfCurrentRound(rows.concat(keep));
  const roundIds = new Set((state.roundIds || []).map(String));
  const authTitles = new Set(rows
    .filter((t) => roundIds.has(String(t.task_id))
      || (t.parent_task_id && roundIds.has(String(t.parent_task_id))))
    .map((t) => String(t.title || '')));
  return merged.filter((t) => !t.__plan || !authTitles.has(String(t.title || '')));
}

/* 【Bug2 修复】切换会话时丢弃"仅存在于本地"的任务行（乐观行 / plan 占位行），
   避免上一会话的占位行泄漏到新会话的任务栏。 */
function dropLocalOnlyTaskRows() {
  state.tasks = (state.tasks || []).filter((t) => !t.__optimistic && !t.__plan);
}

/* 【需求点 Bug3】乐观任务行：用户发送后立即出现在任务面板（status=running），
   后端返回后由 refreshSessionData / refreshTaskPanel 用权威列表覆盖 */
function upsertOptimisticTask(title, taskId) {
  if (taskId && !(state.roundIds || []).includes(String(taskId))) {
    state.roundIds = [String(taskId)];      // 【新增需求2】新消息 = 新的一轮
  }
  state.tasks = (state.tasks || []).filter((t) => t.task_id !== taskId);
  state.tasks.push({
    task_id: taskId, title: title || '（仅附件）', agent_role: '调度规划Agent',
    status: 'running', parent_task_id: '', iteration: 0,
    created_at: Date.now() / 1000, __optimistic: true,
  });
  state.optimisticTaskId = taskId;
  state.taskPanelSig = null;
  renderTasks();
}

/* ====================================================================
   【Bug2 修复】plan 事件 → 任务栏第一时间渲染全部拆解出的子任务
   --------------------------------------------------------------------
   后端在调度规划Agent 拆解完成的第一时间推送 plan 事件（含 subtasks 清单）。
   历史缺陷：plan 事件只进思维链渲染，任务栏行仅来自 /api/session 轮询，
   而轮询要等阻塞式 POST 返回后才启动 → 任务栏滞后整轮任务时长。
   现在：plan 事件到达即插入"占位行"（__plan: true，status=pending）；
   后续 dispatch / subtask_done / 终态事件实时推进占位行状态，
   权威任务行落库后由 refreshTaskPanel 按同标题替换（不产生重复行）。
   ==================================================================== */
function applyPlanSubtasks(evt, taskId) {
  const detail = (evt && evt.detail) || {};
  const subtasks = Array.isArray(detail.subtasks) ? detail.subtasks : [];
  const rootId = String(taskId || state.chainTaskId || state.runningTaskId || '');
  if (!subtasks.length || !rootId) return;
  const others = (state.tasks || []).filter((t) => !t.__plan);
  const planRows = subtasks.map((s, i) => ({
    task_id: `plan-${rootId}-${s.index != null ? s.index : i}`,
    title: String(s.title || `子任务 ${i + 1}`),
    agent_role: String(s.agent_role || ''),
    status: 'pending',
    parent_task_id: rootId,
    iteration: 0,
    created_at: Date.now() / 1000,
    __plan: true,
  }));
  state.tasks = others.concat(planRows);
  state.taskPanelSig = null;
  queueTasksRender();
}

/* 【Bug2 修复】dispatch / subtask_done / 审批事件 → 实时推进占位行状态。
   dispatch 携带 subtask_index + subtask_task_id：占位行认领权威任务号后，
   后续权威列表轮询会自然按 task_id 合并，不会出现双行。 */
function applySubtaskRowEvent(evt, taskId) {
  const detail = (evt && evt.detail) || {};
  const type = (evt && evt.event) || '';
  const rows = state.tasks || [];
  if (type === 'dispatch' && detail.subtask_index != null) {
    const pseudoId = `plan-${taskId}-${detail.subtask_index}`;
    const row = rows.find((t) => t.task_id === pseudoId);
    if (row) {
      row.status = 'running';
      if (detail.subtask_task_id) row.task_id = String(detail.subtask_task_id);
      state.taskPanelSig = null;
      queueTasksRender();
    }
    return;
  }
  if (type === 'subtask_done' && detail.subtask_task_id) {
    const row = rows.find((t) => t.task_id === String(detail.subtask_task_id));
    if (row) {
      const s = String(evt.status || '');
      row.status = ['success', 'failed', 'waiting_approval'].includes(s) ? s
        : (row.status === 'waiting_approval' ? 'waiting_approval' : 'success');
      state.taskPanelSig = null;
      queueTasksRender();
    }
  }
}

/* 【Bug2 修复】任务终态：尚未执行的占位行跟随收敛（成功→success / 失败→failed），
   避免任务已结束但占位行永久停留在 pending。 */
function settlePlanRows(status) {
  let changed = false;
  (state.tasks || []).forEach((t) => {
    if (t.__plan && t.status !== status) { t.status = status; changed = true; }
  });
  if (changed) { state.taskPanelSig = null; queueTasksRender(); }
}

/* 【需求点 Bug3】轮询期间刷新任务面板（后端权威状态 + 保留尚未落库的乐观行）。
   状态指纹未变化时不重绘，避免无意义的 DOM 抖动 */
async function refreshTaskPanel() {
  const sid = state.currentSessionId;
  if (!sid) { renderTasks(); return; }
  try {
    const data = await api(`/api/session/${encodeURIComponent(sid)}`);
    /* 【Bug2 修复】权威行与本地行（乐观行 / plan 占位行）合并，占位行按标题去重 */
    state.tasks = mergeAuthoritativeTasks(data.tasks || []);
    const sig = state.tasks.map((t) => `${t.task_id}:${t.status || ''}`).join('|');
    if (sig !== state.taskPanelSig) { state.taskPanelSig = sig; renderTasks(); }
  } catch (e) { /* 静默：任务面板刷新失败不影响主流程 */ }
}

/* ====================================================================
   任务面板（第7.3 顶部任务面板：可展开/收起、任务状态）
   ==================================================================== */
let taskPanelCollapsed = false;

function renderTasks() {
  const list = state.tasks || [];
  /* 【需求点 Bug3】记录任务面板指纹（task_id:status），供轮询比对，状态无变化时不重复重绘 */
  state.taskPanelSig = list.map((t) => `${t.task_id}:${t.status || ''}`).join('|');
  $('taskBadge').textContent = list.length;
  const wrap = $('taskList');
  if (!list.length) {
    wrap.innerHTML = `<div style="padding:16px;text-align:center;color:var(--text-faint);font-size:12.5px;">暂无任务</div>`;
    return;
  }
  wrap.innerHTML = list.map((t) => {
    const status = t.status || 'pending';
    const spin = status === 'running' ? '<span class="spinner"></span>' : '';
    const sub = t.parent_task_id ? '' : '';
    /* 【BUG-C 2】失败任务行：追加红色错误标识（❌），前端不再显示成功语义 */
    const failMark = status === 'failed' ? '<span class="task-fail-mark">❌</span>' : '';
    /* 【bug-C 4】失败原因摘要（后端已把可读故障原因落在 error_message） */    const reason = status === 'failed' && t.error_message
      ? `<div class="task-fail-reason">${escapeHtml(String(t.error_message).slice(0, 160))}</div>`
      : '';
    /* 【需求点 Bug3】乐观任务行（本地已插入、后端尚未落库）加高亮类，便于识别"刚发出的任务" */
    return `
      <div class="task-item clickable${t.__optimistic ? ' optimistic' : ''}${status === 'failed' ? ' task-failed' : ''}" data-task="${escapeHtml(t.task_id)}">
        <span class="task-name" data-tip="${escapeHtml(t.title)}">${failMark}${escapeHtml(t.title)}
          <span class="task-sub">${escapeHtml(t.agent_role || '')}${t.iteration ? ' · ' + t.iteration + ' 轮' : ''}${sub}</span>
        </span>
        <span class="task-status ${status}">${spin}${STATUS_LABEL[status] || status}</span>
        ${reason}
      </div>`;
  }).join('');
}

/* ====================================================================
   消息区（第7.3：AI思考过程（可折叠、分步图标、连接线）+ 正式回复）
   ==================================================================== */
/* 【需求点 Bug2】streaming=true 时复用同一组件渲染"运行中的流式思维链"：
   仅追加 .streaming 修饰类（实时呼吸点 / 不限制最大高度 / 新步骤淡入），
   静态历史思维链的视觉与折叠行为完全不变 */
function renderThinking(steps, collapsed, streaming) {
  if (!steps || !steps.length) return '';
  /* 【BUG-C 2/5】存在错误步骤时：标题追加「含 N 处错误」，
     对应步骤渲染红色错误标识 + 「发生错误」标签，禁止用 success 状态掩盖失败 */
  const errorCount = steps.filter(isErrorStep).length;
  return `
    <div class="thinking-block${streaming ? ' streaming' : ''}${errorCount ? ' has-error' : ''}">
      <div class="thinking-header" data-toggle-thinking>
        <span>💭 思考过程（${steps.length} 步${streaming ? '，进行中' : ''}${
          errorCount ? `，<span class="think-error-count">含 ${errorCount} 处错误</span>` : ''
        }）</span>
        <span class="chevron ${collapsed ? 'collapsed' : ''}">⌄</span>
      </div>
      <div class="thinking-chain ${collapsed ? 'collapsed' : ''}">
        ${steps.map((s) => {
          const bad = isErrorStep(s);
          return `
          <div class="thinking-step${bad ? ' step-error' : ''}">
            <span class="step-icon">${bad ? '❌' : (THINK_ICON[s.type] || '•')}</span>
            <span class="step-label">${bad ? '发生错误' : (THINK_LABEL[s.type] || s.type)}</span>
            <span class="step-agent">${escapeHtml(s.agent || '')}</span>・${escapeHtml(s.text)}
          </div>`;
        }).join('')}
      </div>
    </div>`;
}

/* 【BUG-C 2】任务条目错误提示：从思考链 / 子任务结果中提取错误条目 */
function errorStepsOf(steps) {
  return (steps || []).filter(isErrorStep);
}

/* 【BUG-C 4】故障诊断卡片（调度规划Agent 输出的结构化故障详情） */
function renderFaultReport(fault) {
  if (!fault) return '';
  const failures = fault.failed_subtasks || [];
  const blocked = fault.blocked_subtasks || [];
  const rows = failures.map((f) => `
    <div class="fault-row">
      <div class="fault-head">
        <span class="fault-badge">失败</span>
        <b>子任务 #${escapeHtml(String(f.index))} · ${escapeHtml(f.title || '')}</b>
      </div>
      <div class="fault-line">执行 Agent：${escapeHtml(f.agent_role || '')}</div>
      ${f.failure_stage ? `<div class="fault-line">失败阶段：<code>${escapeHtml(f.failure_stage)}</code></div>` : ''}
      <div class="fault-line">重试情况：${escapeHtml(String(f.retry_count || 0))} 次${
        f.retries_exhausted ? '（<b>已达到重试上限并终止</b>）' : ''}</div>
      <div class="fault-line fault-reason">失败原因：${escapeHtml(f.failure_reason || f.error || '')}</div>
    </div>`).join('');
  const blockedRows = blocked.map((b) => `
    <div class="fault-row blocked">
      <div class="fault-head"><span class="fault-badge warn">阻断</span>
        <b>子任务 #${escapeHtml(String(b.index))} · ${escapeHtml(b.title || '')}</b></div>
      <div class="fault-line">因上游子任务 ${escapeHtml(JSON.stringify(b.blocked_by || []))} 失败，
        依赖输出无效，已按链路阻断规则停止下发</div>
    </div>`).join('');
  return `
    <div class="fault-report">
      <div class="fault-title">🛑 ${escapeHtml(fault.summary || '任务执行失败：链路已中断')}</div>
      ${rows}
      ${blockedRows}
      <div class="fault-note">${escapeHtml(fault.note || '')}</div>
    </div>`;
}

function renderSubtaskCards(items) {
  if (!items || !items.length) return '';
  return items.map((s) => `
    <div class="subtask-card">
      <div class="sc-head">
        <span class="sc-agent">${escapeHtml(s.agent_role || '')}</span>
        <span class="task-status ${escapeHtml(s.status || 'pending')}" style="font-size:10.5px;padding:2px 8px;">
          ${escapeHtml(STATUS_LABEL[s.status] || s.status || '')}
        </span>
      </div>
      <div style="font-size:12px;color:var(--text);margin-bottom:5px;">${escapeHtml(s.title || '')}</div>
      <div class="sc-body">${escapeHtml((s.output || s.error || '（无输出）').slice(0, 1200))}</div>
    </div>`).join('');
}

/* 【BUG-C 4】从链路事件快照里取出调度规划Agent 的故障诊断 */
function faultReportForTask(taskId) {
  const events = (taskId && state.chainCache[taskId]) || [];
  for (let i = events.length - 1; i >= 0; i -= 1) {
    const d = (events[i] || {}).detail || {};
    if (d.fault_report) return d.fault_report;
  }
  return null;
}

function renderMessages() {
  const wrap = $('messages');
  const items = [];

  (state.messages || []).forEach((m) => {
    const isUserIn = m.sender_agent === 'user' && m.msg_type === 'task';
    const isDelivery = m.receiver_agent === 'user' && (m.msg_type === 'result' || m.msg_type === 'error');
    const isAgent = !isUserIn && m.msg_type !== 'result' && m.msg_type !== 'error';

    if (isUserIn) {
      items.push(`
        <div class="msg user">
          <div>
            <div class="msg-bubble">${escapeHtml(m.content)}</div>
            <div class="msg-time">${nowStr(m.timestamp)}</div>
          </div>
        </div>`);
      return;
    }
    if (isDelivery) {
      /* 【需求点 Bug2】历史交付消息的思维链：优先用链路快照（/api/task/{id}/chain 补齐，
         含分派/工具调用等完整事件），无快照时回退消息自带 think_steps
         （会话接口把 think_steps 放在 metadata 里，兼容两种结构） */
      const steps = stepsForTask(m.task_id, m);
      const subtasks = (m.metadata && m.metadata.subtask_evaluation) || [];
      const failed = m.msg_type === 'error' || m.status === 'failed';
      /* 【BUG-C 2/4】失败任务：消息时间行展示红色错误标识 + 故障诊断卡片 */
      const errs = errorStepsOf(steps);
      const fault = faultReportForTask(m.task_id);
      const errBar = (failed || errs.length)
        ? `<div class="task-error-bar">❌ ${escapeHtml(
            (errs.length && errs[errs.length - 1].text)
            || (m.error_message || '任务执行失败'))}</div>`
        : '';
      items.push(`
        <div class="msg ai${failed ? ' msg-failed' : ''}">
          <div class="msg-bubble">
            <div class="msg-card">
              ${errBar}
              ${renderThinking(steps, false)}
              ${renderFaultReport(fault)}
              ${renderSubtaskCards(subtasks)}
              <div class="reply-body">${formatText(m.content)}</div>
            </div>
            <div class="msg-time">${nowStr(m.timestamp)}${failed ? ' · ❌ 任务失败' : ''}</div>
          </div>
        </div>`);
      return;
    }
    if (isAgent) {
      items.push(`
        <div class="msg-system ${m.status === 'failed' ? 'danger' : (m.status === 'waiting_approval' ? 'warn' : '')}">
          ${escapeHtml(m.sender_agent)} → ${escapeHtml(m.receiver_agent)} · ${escapeHtml(m.msg_type)} · ${escapeHtml(STATUS_LABEL[m.status] || m.status)}
        </div>`);
    }
  });

  if (state.running) {
    /* 【需求点 Bug2】运行中的 AI 消息：用 SSE 实时思维链渲染（复用 renderThinking，
       默认展开），不再只显示一行静态"多 Agent 正在协同执行…"；同时保留转圈提示。
       链路事件尚未到达时自动回退到原有的 state.liveSteps，行为不退化 */
    const chainSteps = (state.chainEvents && state.chainEvents.length)
      ? state.chainEvents.map(chainEventToStep)
      : (state.liveSteps || []);
    items.push(`
      <div class="msg ai" id="liveMsg">
        <div class="msg-bubble">
          <div class="msg-card">
            ${chainSteps.length ? renderThinking(chainSteps, !!state.liveChainCollapsed, true) : ''}
            <div class="reply-body"><p style="color:var(--text-faint)">
              <span class="spinner"></span> 多 Agent 正在协同执行…（任务 ${escapeHtml(((state.runningTaskId || state.chainTaskId || '')).slice(0, 8))}）
            </p></div>
          </div>
        </div>
      </div>`);
  }

  /* 【需求点 Bug8】高危审批卡片内联渲染在消息列表末尾：
     即「#messages 的最新一项」，紧贴用户输入框上方，不再使用独立弹窗 */
  const inlineApprovalsHtml = renderInlineApprovalCards();
  if (inlineApprovalsHtml) items.push(inlineApprovalsHtml);

  if (!items.length) {
    items.push(`<div class="msg-system">开始新对话：描述你的需求，调度规划Agent 会拆解任务并分发给合适的 Agent 执行。<br>
      涉及文件删除 / 批量修改 / 系统命令 / 外网下载时会自动暂停，并在本对话区推送人工审批卡片。</div>`);
  }

  wrap.innerHTML = items.join('');
  wrap.scrollTop = wrap.scrollHeight;
}

function scrollMessagesToBottom() {
  const wrap = $('messages');
  wrap.scrollTop = wrap.scrollHeight;
}

/* ====================================================================
   审批页面（第7.2：审批记录列表、筛选、清空、状态展示）
   ==================================================================== */
async function loadApprovals() {
  if (!state.auth.authenticated && !state.auth.localBypass) return;
  try {
    const q = state.approvalScope === 'session' && state.currentSessionId
      ? `?session_id=${encodeURIComponent(state.currentSessionId)}` : '';
    const data = await api('/api/approval/list' + q);
    state.approvals = data.records || [];
  } catch (e) {
    if (e.status !== 401) toast(`审批记录加载失败：${e.message}`);
  }
  renderApprovals();
}

function renderApprovals() {
  const listEl = $('approvalList');
  $('btnApprovalScope').textContent = state.approvalScope === 'session' ? '仅本会话' : '全部会话';
  const records = state.approvals || [];
  const pendingCount = records.filter((r) => r.state === 'pending').length;
  $('approvalDot').classList.toggle('hidden', pendingCount === 0);

  if (!records.length) {
    listEl.innerHTML = `<div class="approval-empty">${state.approvalScope === 'session' ? '本会话暂无审批记录' : '暂无审批记录'}</div>`;
    return;
  }
  listEl.innerHTML = records.map((r) => `
    <div class="approval-item ${r.state === 'pending' ? 'pending' : ''}">
      <div class="approval-left">
        <span class="approval-type">
          ${escapeHtml(r.operation_type || '高危操作')}
          <span class="risk-tag ${escapeHtml(r.risk_level || 'high')}">${escapeHtml((r.risk_level || 'high').toUpperCase())}</span>
        </span>
        <span class="approval-detail">${escapeHtml(r.operation_desc || '')}</span>
        ${r.operation_params ? `<div class="approval-params">${escapeHtml(r.operation_params)}</div>` : ''}
        ${r.danger_reason ? `<span class="approval-detail">风险说明：${escapeHtml(r.danger_reason)}</span>` : ''}
        <span class="approval-time">${escapeHtml(r.created_at_text || nowStr((r.created_at || 0) * 1000))} · Agent ${escapeHtml(r.agent_role || '')} · 任务 ${escapeHtml((r.task_id || '').slice(0, 8))}</span>
      </div>
      <div class="approval-right">
        <span class="approval-status ${APPROVAL_STATUS_CLASS[r.state] || ''}">${escapeHtml(APPROVAL_STATUS_LABEL[r.state] || r.state)}</span>
        ${r.state === 'pending' ? `
          <div class="approval-btns">
            <button class="mini-btn confirm" data-approve="${escapeHtml(r.approval_id)}">通过</button>
            <button class="mini-btn reject" data-reject="${escapeHtml(r.approval_id)}">拒绝</button>
          </div>` : ''}
      </div>
    </div>`).join('');
}

/* 提交审批结果（第8章 POST /api/approval/submit）
   【新增】入参口径：task_id + outcome（allowed_once / rejected）；
   同时保留 approval_id / approved / risk_level / params_fingerprint 兼容字段。
   【缺陷修复】后端默认 wait_resume=false（立刻返回受理回执），
   因此这里提交后立即结束「提交中…」并转入轮询收口，绝不长时间挂住 UI。 */
async function submitApproval(approvalId, approved) {
  const record = (state.approvals || []).find((r) => r.approval_id === approvalId)
    || (state.inlineApprovals || []).find((r) => r.approval_id === approvalId)   /* 【需求点 Bug8】内联注入的记录同样参与提交参数回填 */
    || {};
  const outcome = approved ? 'allowed_once' : 'rejected';
  let data = null;
  try {
    data = await api('/api/approval/submit', {
      method: 'POST',
      body: {
        /* 需求指定入参 */
        task_id: record.task_id || record.subtask_task_id || null,
        outcome,
        /* 精确审批单 + 后端二次校验所需的兼容字段 */
        approval_id: approvalId,
        approved: Boolean(approved),
        session_id: record.session_id || state.currentSessionId,
        risk_level: record.risk_level || null,
        params_fingerprint: record.params_fingerprint || null,
      },
    });
  } catch (e) {
    toast(`审批提交失败：${e.message}`);
    await loadApprovals();
    throw e;
  }

  clearApprovalDeadline(approvalId);
  toast(approved ? '审批已受理，正在从任务快照断点继续执行（执行链路限时 30 秒）'
                 : '已拒绝，该子任务已终止并回传队长');
  if (data.detail) toast(data.detail);
  await loadApprovals();
  /* 提交后 → 计时已由后端开启：把本条审批标记为"计时中"，立即显示倒计时 */
  const submitted = (state.approvals || []).find((r) => r.approval_id === approvalId)
    || (state.inlineApprovals || []).find((r) => r.approval_id === approvalId);
  if (submitted) {
    submitted.resume_state = String(data.resume_state || 'running');
    submitted.resume_deadline = data.resume_deadline || submitted.resume_deadline;
    submitted.remaining_seconds = Number(data.timeout_seconds || 30);
    submitted.resume_started_at = data.resume_started_at;
    watchApprovalDeadline(submitted);
  }

  /* 非阻塞模式：后台恢复执行中（不等待整轮模型调用），轮询到终态再收口 */
  if (data.resume_mode === 'async') {
    state.running = true;
    state.runningTaskId = data.task_id || state.runningTaskId;
    /* 【第三轮·Bug2】轮询带上后端给的 30 秒窗口，避免"前端比后端多等 150 秒"的假卡死 */
    pollApprovalUntilSettled(approvalId, data)
      .then(() => { state.running = false; refreshTaskPanel(); refreshStatus(); })
      .catch(() => { state.running = false; });
    return data;
  }

  await refreshSessionData();
  const resumedTaskId = (data && (data.task_id || (data.execution || {}).task_id)) || '';
  if (approved && resumedTaskId) {
    state.running = true;
    state.runningTaskId = resumedTaskId;
    state.cancelTaskId = resumedTaskId;
    startPolling(resumedTaskId);
  } else {
    await refreshTaskPanel();
  }
  return data;
}

/* ====================================================================
   状态栏（第7.3 状态栏 + 第2章 2.4：全部为后端真实数据）
   ==================================================================== */
function fmtTok(n) {
  const v = Number(n || 0);
  return v >= 1000 ? (v / 1000).toFixed(1) + 'K' : String(v);
}
function fmtMs(ms) {
  const s = Number(ms || 0) / 1000;
  if (s < 60) return `${s.toFixed(1)}秒`;
  const m = Math.floor(s / 60);
  return `${m}分${String(Math.floor(s % 60)).padStart(2, '0')}秒`;
}

function renderStatusBar() {
  /* 【需求点 Bug9】优先使用「当前 task」的后端真实统计；无当前任务时退回会话维度；
     两者都没有 → 全部清零（无任务时清空统计面板，绝不展示旧任务残留数据） */
  const hasTaskScope = Boolean(state.currentTaskStats && state.runningTaskId);
  const s = state.stats || {};
  const session = hasTaskScope ? state.currentTaskStats : (s.session || {});
  const cacheRate = Number(session.cache_hit_rate || 0);
  const inputTok = Number(session.input_tokens || 0);
  const outputTok = Number(session.output_tokens || 0);
  const elapsed = Number(session.elapsed_ms || 0);
  const steps = Number(session.steps || 0);
  const calls = Number(session.calls || 0);

  /* 【需求点 三、2】状态栏展示当前实际运行的模型名称：
     有补位记录则展示补位后的实际模型，并高亮提示 */
  const fb = (state.ecosystemFallbacks || [])[0];
  const dispatchModel = (state.agentModels || []).find(
    (m) => m.agent === '调度规划Agent' || m.agent === '调度规划Agent') ||
    (state.agentModels || [])[0];
  let modelHtml = '';
  if (fb) {
    modelHtml = `<span class="model-actual fallback">生态位补位 <b>${escapeHtml(fb.actual_model)}</b>` +
      `（原 ${escapeHtml(fb.original_model)}）</span>`;
  } else if (dispatchModel) {
    modelHtml = `<span class="model-actual">当前 <b>${escapeHtml(dispatchModel.actual_model_label || '')}</b></span>`;
  } else {
    const hint = state.modelHint && state.modelHint !== 'auto'
      ? (MODEL_OPTIONS.find((m) => m.id === state.modelHint) || {}).name
      : '自动编排';
    modelHtml = `<span class="model-actual">当前 <b>${escapeHtml(hint || '自动编排')}</b></span>`;
  }

  $('statusLeft').innerHTML =
    `${calls} 次调用<span class="sep">·</span>${steps} 步<span class="sep">|</span>` +
    `LLM 耗时 ${fmtMs(elapsed)}` +
    (state.running ? '<span class="sep">·</span>执行中…' : '') +
    (hasTaskScope ? '<span class="sep">·</span>当前任务' : '');
  $('statusRight').innerHTML =
    modelHtml +
    `<span class="sep">|</span>缓存命中 ${cacheRate}%` +
    `<span class="sep">|</span>输入 ${fmtTok(inputTok)} tok<span class="sep">·</span>输出 ${fmtTok(outputTok)} tok`;
}

async function refreshStatus() {
  try {
    // 【需求点 Bug9】任务执行中带上当前活跃 task_id：后端按该任务返回真实统计；
    // 无运行中任务时不带 task_id → 展示会话累计值（仍为后端真实数据）。
    const params = [];
    if (state.currentSessionId) params.push(`session_id=${encodeURIComponent(state.currentSessionId)}`);
    if (state.running && state.runningTaskId) {
      params.push(`task_id=${encodeURIComponent(state.runningTaskId)}`);
    }
    const data = await api('/api/status' + (params.length ? `?${params.join('&')}` : ''));
    state.status = data;
    state.stats = { session: data.session_tokens || {} };
    // 【需求点 Bug9】当前运行 task 维度的真实统计
    state.currentTaskStats = (!state.running || !state.runningTaskId) ? null : (data.task_tokens || null);
    // 【需求点 三、2】记录本次的补位事件与七大 Agent 实跑模型
    state.ecosystemFallbacks = data.ecosystem_fallbacks || [];
    state.agentModels = data.agent_models || [];
    renderStatusBar();
    renderSysInfo(data);
    renderDegrade(data.degradation || {});
  } catch (e) {
    if (e.status !== 401) { /* 静默：状态栏刷新失败不打扰用户 */ }
  }
}

/* 仅刷新状态数据的轻量版本（供开关切换后调用） */
async function loadStatus() { return refreshStatus(); }

/* ====================================================================
   【需求点 Bug9】底部统计栏实时刷新
     · 前端每 3 秒向后端轮询一次当前 task 的统计数据（/api/status）
     · 实时更新：调用次数 / 步数 / LLM 耗时 / 缓存命中率 / 输入 token / 输出 token
     · 无任务（无会话）时清空统计面板
     · 切换会话 / 切换工作区时重置统计面板并立即重新拉取
     · 数据严格来自后端接口，前端不生成任何模拟值
   ==================================================================== */
const STATS_POLL_INTERVAL_MS = 3000;

function resetStatsPanel() {
  state.stats = null;
  state.status = null;
  state.currentTaskStats = null;
  renderStatusBar();
}

function startStatsPolling() {
  if (state.statsPollTimer) return;      // 已在轮询，避免重复定时器
  state.statsPollTimer = setInterval(async () => {
    if (document.hidden) return;         // 页面不可见时跳过，减少无谓请求
    await refreshStatus();
  }, STATS_POLL_INTERVAL_MS);
}

function stopStatsPolling() {
  if (state.statsPollTimer) {
    clearInterval(state.statsPollTimer);
    state.statsPollTimer = null;
  }
}

/* 切换会话 / 切换工作区：重置统计面板 + 立即按新会话重新拉取真实统计 */
async function resetAndRefreshStats() {
  resetStatsPanel();
  await refreshStatus();
}

/* ====================================================================
   【需求点 Bug8 / BUG-NEW2】左下角状态栏：逐条列出全部 7 个 Agent 及其绑定的模型
     数据来源：后端 /api/status → agent_models（与 AGENT_BINDINGS 同源，绝不前端硬编码）
     顺序与《最终固定 Agent-模型绑定映射》表格一致：
       代码工程 Agent → DeepSeek-Flash
       文档信息 Agent → Kimi K3
       视觉感知 Agent → Qwen3.8-Flash
       调度规划 Agent → Qwen3.8-Max
       评估校验 Agent → GLM 5.3
       交互交付 Agent → Kimi k2.6
       记忆管理 Agent → GLM-5.3-Flash
   ==================================================================== */
const AGENT_DISPLAY_ORDER = [
  '代码工程Agent', '文档信息Agent', '视觉感知Agent', '调度规划Agent',
  '评估校验Agent', '交互交付Agent', '记忆管理Agent',
];

function renderAgentBindingLines(data) {
  const runtime = data.agent_models || [];
  const cfgMap = (state.config && state.config.agent_model_map) || [];
  const byAgent = {};
  runtime.forEach((m) => { byAgent[m.agent] = m; });
  cfgMap.forEach((m) => { byAgent[m.agent] = { ...(byAgent[m.agent] || {}), ...m }; });

  const order = AGENT_DISPLAY_ORDER.filter((a) => byAgent[a])
    .concat(Object.keys(byAgent).filter((a) => !AGENT_DISPLAY_ORDER.includes(a)));

  if (!order.length) {
    // 后端尚未返回明细时，退回配置面板的映射表（仍为后端数据）
    return `<div class="sys-agent-empty">Agent 绑定明细加载中…</div>`;
  }

  return order.map((agent) => {
    const m = byAgent[agent] || {};
    const label = m.model_name || m.assigned_model_name || m.actual_model_label || '—';
    const bound = m.bound_model || m.model || '';
    const fallback = m.ecosystem_fallback
      ? ` <span class="sys-fallback">（生态位补位：${escapeHtml(m.actual_model_label || '')}）</span>`
      : '';
    const shortName = agent.replace(/Agent$/, ' Agent');
    return `<div class="sys-agent-line" data-tip="${escapeHtml(
      (m.duty ? `职责：${m.duty}\n` : '') + (bound ? `平台模型标识：${bound}` : '')
    )}">·${escapeHtml(shortName)} → <b>${escapeHtml(label)}</b>${fallback}</div>`;
  }).join('');
}

function renderSysInfo(data) {
  const v = data.vector_db || {};
  const mem = data.memory || {};
  const bus = data.bus || {};
  const limits = data.limits || {};
  const eco = (data.degradation || {}).ecosystem || {};
  const fallbacks = data.ecosystem_fallbacks || [];
  /* 【需求点 二、Bug2】补位提示行带 data-eco-line 标记，
     使 renderFallbackSysInfo() 能在切换开关后精准同步该行文本 */
  const fbLine = fallbacks.length
    ? `<div data-eco-line><b>生态位补位</b>：${fallbacks.length} 项（${escapeHtml(fallbacks[0].actual_model)}）</div>`
    : `<div data-eco-line><b>生态位补位</b>：${eco.enabled === false ? '已关闭' : '已开启'} · 候选 ${(eco.tested_ok_providers || []).length} 个</div>`;
  /* 【需求点 Bug8】完整逐条列出 7 个 Agent ↔ LLM 绑定 */
  $('sysInfo').innerHTML = `
    <div><b>裸机模式</b>：无 Docker / 无容器</div>
    <div><b>七大Agent</b>：${(data.agents || []).length} 个（模型绑定固定）</div>
    <div class="sys-agent-list">${renderAgentBindingLines(data)}</div>
    ${fbLine}
    <div><b>消息总线</b>：投递 ${bus.delivered || 0} / 发布 ${bus.published || 0}</div>
    <div><b>向量库</b>：${v.available ? '可用' : '已降级关闭'} · 长期记忆 ${v.count || 0} 条</div>
    <div><b>短期记忆</b>：${mem.short_term_entries || 0} 条</div>
    <div><b>硬限制</b>：迭代 ${limits.max_iterations} / 超时 ${Math.round((limits.max_timeout_seconds || 1800) / 60)}分 / 重试 ${limits.max_retries}</div>`;
}

function renderDegrade(dg) {
  const chip = $('degradeChip');
  const notes = [];
  if (dg.dispatch_degraded) notes.push('当前为备选调度模型');
  (dg.ecosystem_fallbacks || []).forEach((f) => {
    notes.push(f.note || `生态位补位：${f.actual_model}`);
  });
  (dg.disabled_agents || []).forEach((a) => notes.push(`${a.model_name} 失效，能力已禁用`));
  if (dg.vector_db_available === false) notes.push('向量库异常，长期记忆已关闭');
  if (notes.length) {
    chip.textContent = notes[0];
    chip.classList.remove('hidden');
  } else {
    chip.classList.add('hidden');
  }
}

/* ====================================================================
   【需求点 二、任务执行计时器】对话框左上角计时组件
   规则（与后端状态机严格对齐）：
     · 任务正式开始提交 → 计时器清零并开始计时（formatElapsed → 已耗时：00:00:00）
     · 任务结束（success / failed）→ 停止计时并展示本次耗时
     · 任务取消 / 终止 → 停止计时并展示已耗时（状态 cancelled）
     · 任务进入人工审批 → 暂停计时（审批通过后继续累计）
     · 没有正在运行的任务 → 默认文字「未开始任务」；
       历史会话打开时展示该会话上一次任务耗时（来自后端会话元数据）
   ==================================================================== */
const TIMER_STATE_LABEL = {
  idle: '未开始任务',
  running: '计时中',
  paused: '已暂停（等待审批）',
  success: '已完成',
  failed: '已结束（失败）',
  cancelled: '已取消',
};
const TIMER_STATE_ICON = {
  idle: '⏱', running: '⏱', paused: '⏸', success: '✓', failed: '⚠', cancelled: '■',
};

/* 【需求点 二、2】计时格式：时:分:秒（HH:MM:SS），前端同样两位补零、小时不封顶 */
function formatElapsed(seconds) {
  const total = Math.max(0, Math.floor(Number(seconds) || 0));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(h)}:${pad(m)}:${pad(s)}`;
}

/* 计时器复位（无会话 / 切换到无计时记录的会话 / 初始化时） */
function resetTimerState() {
  state.timer = {
    timerId: '', sessionId: state.currentSessionId || '', taskId: '', title: '',
    status: 'idle', source: 'idle', elapsed: 0,
    startedAt: null, finishedAt: null, running: false,
  };
  state.timerSyncCounter = 0;
  stopTimerTicker();
  renderTaskTimer();
}

/* 渲染计时器（数据全部来自后端 / 本次任务真实回执，前端不做任何模拟） */
function renderTaskTimer() {
  const box = $('taskTimer');
  if (!box) return;
  const t = state.timer || { status: 'idle', elapsed: 0 };
  const status = t.status || 'idle';

  // 有正在运行的任务 → 计时器实时刷新；否则按状态展示耗时/默认文案
  const running = status === 'running';
  const showElapsed = running || status === 'paused'
    || (['success', 'failed', 'cancelled'].includes(status) && (t.source === 'local' || t.source === 'live'));

  let text;
  if (status === 'idle') {
    // 【需求点 二、2】没有正在运行的任务 → 默认显示文字「未开始任务」
    text = '未开始任务';
  } else if (showElapsed) {
    text = `已耗时：${formatElapsed(t.elapsed)}`;
  } else {
    // 历史会话回显：上一次任务耗时
    text = `已耗时：${formatElapsed(t.elapsed)}`;
  }

  $('timerIcon').textContent = TIMER_STATE_ICON[status] || '⏱';
  $('timerText').textContent = text;

  const sub = $('timerSub');
  const subParts = [];
  if (status === 'running') subParts.push('本次任务 · 计时中');
  else if (status === 'paused') subParts.push('本次任务 · 已暂停（等待审批）');
  else if (['success', 'failed', 'cancelled'].includes(status) && (t.source === 'local' || t.source === 'live')) {
    subParts.push(`本次任务 · ${TIMER_STATE_LABEL[status] || status}`);
  } else if (status !== 'idle') {
    subParts.push(`上一次任务耗时 · ${TIMER_STATE_LABEL[status] || status}`);
  }
  if (subParts.length) {
    sub.textContent = subParts.join('');
    sub.classList.remove('hidden');
  } else {
    sub.textContent = '';
    sub.classList.add('hidden');
  }
  const titleBits = ['任务执行计时器'];
  if (t.title) titleBits.push(`任务：${t.title}`);
  if (t.taskId) titleBits.push(`task_id：${t.taskId}`);
  titleBits.push(`状态：${TIMER_STATE_LABEL[status] || status}`);

  // 【需求点 二、2-c】任务执行中才展示「终止任务」按钮
  const cancelBtn = $('btnCancelTask');
  if (cancelBtn) {
    cancelBtn.classList.toggle('hidden', !state.running);
    cancelBtn.disabled = !state.running;
  }
}

/* 【需求点 二、2-a】任务正式开始提交 → 计时器清零并开始计时（乐观显示，后端为准） */
function startTaskTimer() {
  state.timer = {
    timerId: state.timer.timerId || '', sessionId: state.currentSessionId || '',
    taskId: '', title: '', status: 'running', source: 'local',
    elapsed: 0, startedAt: Date.now(), finishedAt: null, running: true,
  };
  state.timerSyncCounter = 0;
  renderTaskTimer();
  startTimerTicker();
}

/* 用后端返回的计时器视图覆盖本地状态（唯一数据权威） */
function applyTimerFromServer(timer, source) {
  if (!timer || typeof timer !== 'object') return;
  const elapsed = Number(timer.elapsed_seconds || 0);
  state.timer = {
    timerId: timer.timer_id || state.timer.timerId,
    sessionId: timer.session_id || state.currentSessionId || '',
    taskId: timer.task_id || '',
    title: timer.title || '',
    status: timer.status || 'idle',
    source: source || timer.source || 'live',
    elapsed,
    startedAt: timer.started_at ? Number(timer.started_at) * 1000 : state.timer.startedAt,
    finishedAt: timer.finished_at ? Number(timer.finished_at) * 1000 : null,
    running: Boolean(timer.running),
  };
  renderTaskTimer();
}

/* 【需求点 二、2-b/c】任务结束 / 取消 → 停止计时（前端立即停表，后端已落库） */
function stopTaskTimer(result) {
  stopTimerTicker();
  const waiting = result && result.status === 'waiting_approval';
  const fromServer = result && result.task_timer;
  if (fromServer) {
    applyTimerFromServer(fromServer, 'local');
  } else if (waiting) {
    // 【需求点 二、2】任务进入人工审批（暂停态）→ 前端停表，不计入耗时
    state.timer.status = 'paused';
    state.timer.source = 'local';
    state.timer.running = false;
  } else {
    state.timer.status = (result && result.status === 'failed') ? 'failed' : 'success';
    state.timer.source = 'local';
    state.timer.running = false;
    state.timer.finishedAt = Date.now();
  }
  renderTaskTimer();
}

/* 计时器本地 1 秒刷新 + 周期性向后端校准（真实数据，不模拟） */
function startTimerTicker() {
  stopTimerTicker();
  state.timerInterval = setInterval(async () => {
    if (!state.timer.running) return;
    state.timer.elapsed = Number(state.timer.elapsed || 0) + 1;
    renderTaskTimer();
    state.timerSyncCounter += 1;
    if (state.timerSyncCounter >= 3) {
      state.timerSyncCounter = 0;
      await syncSessionTimer();
    }
  }, 1000);
}
function stopTimerTicker() {
  if (state.timerInterval) { clearInterval(state.timerInterval); state.timerInterval = null; }
}

/* 从后端拉取计时器真实状态（历史会话 / 校准） */
async function syncSessionTimer() {
  const sid = state.currentSessionId;
  if (!sid) { resetTimerState(); return; }
  try {
    const data = await api(`/api/session/${encodeURIComponent(sid)}/timers?limit=1`);
    const timer = data.latest || {};
    if (timer.status === 'idle') {
      // 该会话没有任何计时记录 → 默认文字「未开始任务」
      state.timer = {
        timerId: '', sessionId: sid, taskId: '', title: '',
        status: 'idle', source: 'idle', elapsed: 0,
        startedAt: null, finishedAt: null, running: false,
      };
      stopTimerTicker();
      renderTaskTimer();
      return;
    }
    applyTimerFromServer(timer, timer.running ? 'live' : 'history');
    if (timer.running) startTimerTicker(); else stopTimerTicker();
  } catch (e) {
    // 计时器同步失败不影响主流程（静默降级，下一次轮询再试）
  }
}

async function loadSessionTimer() {
  await syncSessionTimer();
}

/* 【需求点 二、2-c】取消 / 终止任务 → 后端停止计时并落库 */
async function cancelRunningTask() {
  const sid = state.currentSessionId;
  if (!sid || !state.running) { toast('当前没有正在执行的任务'); return; }
  const btn = $('btnCancelTask');
  if (btn) btn.disabled = true;
  try {
    const data = await api(`/api/task/cancel?session_id=${encodeURIComponent(sid)}`
      + `&task_id=${encodeURIComponent(state.cancelTaskId || '')}`, { method: 'POST' });
    applyTimerFromServer(data.timer, 'local');
    if (state.timer.status !== 'idle') state.timer.status = 'cancelled';
    state.timer.running = false;
    renderTaskTimer();
    toast('任务已取消，计时已停止');
    // 任务已被后端收敛为 failed → 收敛前端执行态
    state.running = false;
    stopPolling();
    $('btnSend').disabled = false;
    renderTaskTimer();
    await refreshSessionData();
    await refreshStatus();
  } catch (e) {
    toast(`取消失败：${e.message}`);
  } finally {
    if (btn) btn.disabled = !state.running;
  }
}

/* ====================================================================
   发送消息 → 多 Agent 任务链路（第8章 POST /api/session/chat）
   ==================================================================== */
async function sendMessage(textOverride) {
  const input = $('chatInput');
  const text = (typeof textOverride === 'string' ? textOverride : input.value).trim();
  if (!text && !state.attachments.length) return;
  /* 【需求点 Bug2 边界约束1】待审批阻塞：审批未完成前不允许提交新消息 */
  if (state.approvalBlock && state.approvalBlock.active) {
    renderApprovalBlock();
    toast('存在待审批任务，请先完成审批');
    return;
  }
  if (state.running) { toast('当前任务仍在执行，请等待完成'); return; }

  const attachments = state.attachments.map((a) => ({ name: a.name, data_base64: a.data_base64 }));
  if (typeof textOverride !== 'string') {
    input.value = '';
    input.style.height = 'auto';
  }
  state.attachments = [];
  renderAttachments();

  /* 【需求点 Bug1】未选中任何会话（欢迎首页 / 无会话状态）时，本次发送必须新建会话：
     force_new_session + create_session 同时为 true，后端必定新建并把 session_id 回传 */
  const needNewSession = !state.currentSessionId;
  const targetWorkspaceId = state.currentWorkspaceId || null;

  /* 【需求点 Bug2】客户端先生成 task_id：POST 与 SSE 订阅使用同一个 ID，
     才能在这个同步阻塞的 POST 执行期间收到实时思维链 */
  const clientTaskId = genClientTaskId();

  state.running = true;
  state.liveSteps = [];
  state.runningTaskId = clientTaskId;
  state.cancelTaskId = null;
  state.liveChainCollapsed = false;
  /* 【新增需求2】新消息 = 新的一轮：先清空上一轮全部旧任务条目，
     随后队长拆出的本次任务列表会渲染进来（任务栏不再累积历史旧任务）。 */
  clearTaskRoundForNewInput(clientTaskId);
  /* 【需求点 Bug2】重置本次任务的流式链路状态 */
  state.chainEvents = [];
  state.chainSawRealEvent = false;
  state.chainSeq = 0;
  state.chainTaskId = clientTaskId;
  /* 【需求点 Bug1】先行切到对话视图，使新会话的流式思维链立即可见 */
  state.pendingNewSession = needNewSession;
  // 【需求点 二、2-a】任务正式开始提交 → 计时器清零并启动计时
  startTaskTimer();
  /* 【需求点 Bug2】必须先订阅 SSE 再发 POST：POST 会同步阻塞到任务结束 */
  openChainStream(clientTaskId);
  state.messages.push({
    msg_id: 'local_' + Date.now(), task_id: '', task_title: '',
    sender_agent: 'user', receiver_agent: '调度规划Agent', msg_type: 'task',
    status: 'success', timestamp: Date.now(), content: text || '（仅附件）', metadata: {},
  });
  /* 【需求点 Bug3】乐观任务行：发送瞬间任务面板立即出现该任务（进行中），
     后端返回后由权威任务列表覆盖，无需手动刷新 */
  upsertOptimisticTask(text || '（仅附件）', clientTaskId);
  renderMainArea();
  renderMessages();
  $('btnSend').disabled = true;

  try {
    const data = await api('/api/session/chat', {
      method: 'POST',
      body: {
        session_id: state.currentSessionId,
        // 【需求点 Bug1】无会话 / 需要新会话 → 后端强制新建（忽略传入的 session_id）
        force_new_session: needNewSession,
        create_session: needNewSession,
        // 【需求点 二、1】新建会话时归入当前工作区；已有会话传 null，
        // 避免 upsert_session 覆盖其标题与工作区归属
        workspace_id: needNewSession ? targetWorkspaceId : null,
        message: text,
        // 【需求点 Bug2】客户端生成的 task_id（^[A-Za-z0-9_-]{8,64}$）
        task_id: clientTaskId,
        model_hint: state.modelHint === 'auto' ? null : state.modelHint,
        attachments,
      },
    });
    const result = data.result || {};
    /* 【需求点 Bug1】自动建会话：切到后端回传的新 session_id，直接渲染该会话的正常对话视图，
       全程无需手动点「新建会话」 */
    state.currentSessionId = result.session_id || state.currentSessionId;
    state.pendingNewSession = false;
    state.runningTaskId = result.task_id || clientTaskId;
    state.cancelTaskId = result.task_id || null;
    state.liveSteps = result.think_steps || [];
    if (result.status === 'waiting_approval' && result.pending_approval) {
      /* 【需求点 Bug8】高危审批不再弹窗：把 pending_approval 注入内联审批卡片，
         无需手动刷新即可出现在消息区末尾（用户输入框正上方） */
      trackInlineApproval(result.pending_approval);
    }
    /* 【高危审批交互修复】无论响应体是否带 pending_approval，
       只要任务处于等待审批，一律再向后端拉一次权威审批记录并渲染
       【✅同意审批】【❌拒绝审批】按钮 —— 这是审批交互的唯一入口。 */
    if (result.status === 'waiting_approval') {
      await syncPendingApprovals();
    }
    // 任务已受理 → 启动思考过程轮询（后端真实数据）
    if (result.task_id && result.status === 'running') startPolling(result.task_id);
    await afterTask(result);
  } catch (e) {
    state.running = false;
    state.pendingNewSession = false;
    $('btnSend').disabled = false;
    /* 【需求点 Bug2 边界约束1】后端 409 = 会话里仍有待审批任务 → 进入阻塞态并提示 */
    const payload = (e && e.payload) || {};
    if (payload.error === 'PENDING_APPROVAL_EXISTS') {
      enterApprovalBlock({ detail: payload.pending_approval || {} });
      if (payload.pending_approval) trackInlineApproval(payload.pending_approval);
      await syncPendingApprovals().catch(() => {});
      renderMessages();
      renderApprovalBlock();
      toast(payload.message || '存在待审批任务，请先完成审批');
      return;
    }
    /* 【需求点 Bug2】请求失败 → 立刻关闭流式通道与降级轮询，不留悬挂连接 */
    closeChainStream();
    // 【需求点 二、2-c】请求失败 → 任务未真正启动/已终止 → 停止计时（前端不空转）
    stopTaskTimer({ status: 'failed' });
    state.messages.push({
      msg_id: 'err_' + Date.now(), task_id: '', task_title: '',
      sender_agent: 'system', receiver_agent: 'user', msg_type: 'error',
      status: 'failed', timestamp: Date.now(), content: `请求失败：${e.message}`, metadata: {},
    });
    /* 【需求点 Bug3】请求失败 → 乐观任务行收敛为「失败」（不谎报执行中/成功） */
    applyTaskStatus(state.optimisticTaskId || clientTaskId, 'failed');
    state.optimisticTaskId = null;
    renderMessages();
    renderMainArea();
    toast(`请求失败：${e.message}`);
  }
}

async function afterTask(result) {
  state.running = false;
  state.liveSteps = [];
  state.cancelTaskId = null;
  $('btnSend').disabled = false;
  /* 【需求点 Bug2】任务结束 → 停止 SSE 实时流（含降级轮询），
     并把本次链路固化为快照：/chain 的后端事件比本地更完整时以它为准，
     失败的权威消息列表随后由 refreshSessionData 覆盖现场占位消息 */
  closeChainStream();
  const finishedTaskId = state.chainTaskId || (result && result.task_id) || null;
  if (finishedTaskId) {
    if (state.chainEvents.length) state.chainCache[finishedTaskId] = state.chainEvents.slice();
    await loadChainSnapshot(finishedTaskId, true);
  }
  /* 【需求点 Bug2】本任务已是终态，后续不再往 liveMsg 追加步骤 */
  state.chainEvents = [];
  state.chainSawRealEvent = false;
  state.chainSeq = 0;
  state.chainTaskId = null;
  /* 【需求点 Bug3】乐观任务行使命结束：随后的权威任务列表接管任务面板 */
  state.optimisticTaskId = null;

  // 【需求点 二、2-b/c】任务结束（success/failed/取消）→ 停止计时并展示本次耗时
  stopTaskTimer(result || {});

  // 【需求点 三、2】生态位补位 → 弹出非阻断轻提示（不阻塞任何操作）
  const fallbacks = (result && result.ecosystem_fallbacks) || [];
  if (fallbacks.length) showEcoNotice(fallbacks);

  await loadWorkspaces();
  /* 【需求点 Bug3】用后端权威任务列表覆盖乐观任务行（refreshSessionData 内已重绘任务面板） */
  await refreshSessionData();
  renderAll();

  if (result && result.status === 'failed' && result.error_message) {
    // 【需求点 三、3】补位耗尽等失败：给出用户可读提示（常驻展示在消息区 + 轻提示）
    toast(`任务失败：${(result.error_message || '').slice(0, 140)}`);
  }
  await refreshStatus();
}

/* 轮询任务进度（后端真实数据） */
function startPolling(taskId) {
  stopPolling();
  state.pollTimer = setInterval(async () => {
    if (!taskId) return;
    try {
      const data = await api(`/api/task/${encodeURIComponent(taskId)}`);
      /* 【需求点 Bug2】SSE 链路已提供实时步骤时，不再用轮询结果覆盖（避免步骤重复/回退）；
         仅在流式通道不可用（被关闭或降级）时回退到原有 think_steps 轮询渲染 */
      if (data.think_steps && !state.chainSawRealEvent) {
        state.liveSteps = data.think_steps;
        renderMessages();
      }
      /* 【需求点 Bug3】轮询期间同步刷新任务面板（后端状态变化实时可见，无需手动刷新） */
      await refreshTaskPanel();
      const status = (data.task || {}).status;
      if (status === 'waiting_approval') {
        /* 【高危审批交互修复】等待审批期间继续轮询（不再 stopPolling）：
           既保证审批按钮稳定可见（每次同步后端权威记录），
           也保证用户裁决后任务恢复 running → success 能被前端自动察觉。 */
        await syncPendingApprovals().catch(() => {});
      } else if (['success', 'failed'].includes(status)) {
        stopPolling();
      }
    } catch (e) { /* 轮询失败忽略，等待下一轮 */ }
  }, 1500);
}
function stopPolling() {
  if (state.pollTimer) { clearInterval(state.pollTimer); state.pollTimer = null; }
}

/* ====================================================================
   附件（第6章 6.2 文件上传安全限制；第3章 3.3 图片资源）
   ==================================================================== */
function renderAttachments() {
  const strip = $('attachStrip');
  if (!state.attachments.length) { strip.classList.add('hidden'); strip.innerHTML = ''; return; }
  strip.classList.remove('hidden');
  strip.innerHTML = state.attachments.map((a, i) => `
    <span class="attach-chip">${a.kind === 'image' ? '🖼' : '📄'} ${escapeHtml(a.name)}
      <span style="color:var(--text-faint)">${(a.size / 1024).toFixed(0)}KB</span>
      <button data-remove="${i}">✕</button></span>`).join('');
}

async function handleFiles(fileList) {
  const MAX = 50 * 1024 * 1024;
  for (const file of Array.from(fileList || [])) {
    if (file.size > MAX) { toast(`「${file.name}」超过 50MB 上限，已跳过`); continue; }
    const ext = (file.name.match(/\.[^.]+$/) || [''])[0].toLowerCase();
    if (['.exe', '.bat', '.sh', '.bin', '.cmd', '.ps1', '.msi', '.dll', '.jar'].includes(ext)) {
      toast(`禁止上传可执行文件：${file.name}`);
      continue;
    }
    const dataBase64 = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result).split(',')[1] || '');
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
    state.attachments.push({
      name: file.name, size: file.size, data_base64: dataBase64,
      kind: /^image\//.test(file.type) ? 'image' : 'file',
    });
  }
  renderAttachments();
  toast('附件已就绪，发送时将由后端安全校验并隔离到当前会话目录');
}

/* ====================================================================
   【需求点 Bug8】高危审批：内联审批卡片（原弹窗已彻底移除）
   · 审批内容直接出现在消息区最新一条（用户输入框正上方），不再使用任何弹窗
   · 前端只负责「展示 + 提交」，执行与否一律由后端二次校验后裁决，前端无法绕过
   ==================================================================== */

/* 【需求点 Bug8】裁决后的结果文案（本页面会话内保留展示，让用户看到自己的选择）
   【新增】timeout：30 秒审批超时自动拒绝（等价 rejected，绝不放行高危操作） */
const INLINE_DECIDED_LABEL = {
  manual: '✅ 已通过（人工审批，后端二次校验通过后执行）',
  auto: '✅ 已放行（后端自动校验通过）',
  rejected: '⛔ 已拒绝（该子任务已终止，未执行任何操作）',
  timeout: '⏱ 审批超时自动拒绝（30 秒无操作，等价「拒绝」，未执行任何操作）',
};

/* ====================================================================
   【需求点 Bug2 边界约束1】待审批阻塞态
   --------------------------------------------------------------------
   后端的业务循环在命中高危操作时会中断任务（waiting_approval）：
     1) SSE 停止流式输出（stream_paused）；
     2) 推送 approval_request 事件；
     3) 前端在输入框上方渲染【✅ 执行一次】【❌ 拒绝】按钮；
     4) **临时阻塞普通输入框提交**，提示"存在待审批任务，请先完成审批"，
        防止新旧任务互相干扰（后端同样会 409 拦截，前端只是提前提示）。
   审批裁决后（approval_result 事件 / 提交成功）自动退出阻塞态。
   ==================================================================== */
function enterApprovalBlock(evt) {
  const detail = (evt && evt.detail) || {};
  const approvalId = String(detail.approval_id || state.approvalBlock.approvalId || '');
  state.approvalBlock = {
    active: true,
    approvalId,
    taskId: String(detail.root_task_id || detail.subtask_task_id || (evt && evt.task_id) || ''),
    text: '存在待审批任务，请先完成审批',
  };
  renderApprovalBlock();
}

function exitApprovalBlock() {
  state.approvalBlock = { active: false, approvalId: '', taskId: '', text: '' };
  renderApprovalBlock();
}

/* 依据当前会话的后端权威审批记录同步阻塞态（刷新页面 / 切会话后仍能正确恢复） */
function syncApprovalBlockFromRecords() {
  const sid = state.currentSessionId;
  const pending = (state.approvals || []).filter((r) => {
    if (!r || r.state !== 'pending') return false;
    return !sid || !r.session_id || r.session_id === sid;
  });
  if (pending.length) {
    enterApprovalBlock({ detail: { approval_id: pending[0].approval_id,
                                   subtask_task_id: pending[0].task_id } });
  }
}

function renderApprovalBlock() {
  const banner = $('approvalBlockBanner');
  const textEl = $('approvalBlockText');
  const blocked = Boolean(state.approvalBlock && state.approvalBlock.active);
  if (banner && textEl) {
    banner.classList.toggle('hidden', !blocked);
    textEl.textContent = blocked
      ? `${state.approvalBlock.text}（请点击上方【✅ 执行一次】或【❌ 拒绝】）`
      : '';
  }
  const sendBtn = $('btnSend');
  const input = $('chatInput');
  if (sendBtn) sendBtn.disabled = blocked || Boolean(state.running);
  if (input) {
    input.classList.toggle('input-blocked', blocked);
    input.placeholder = blocked
      ? '存在待审批任务，请先完成审批后再发送新消息'
      : '发消息或做任务... / 调用指令 @ 文件或对话';
  }
}

/* 会话切换 / 回到欢迎页时清空阻塞态（新会话没有历史待审批） */
function resetApprovalBlock() {
  state.approvalBlock = { active: false, approvalId: '', taskId: '', text: '' };
  renderApprovalBlock();
}


/* 【需求点 Bug8】把一条审批记录注入内联卡片区（按 approval_id 去重） */
function trackInlineApproval(record) {
  if (!record || !record.approval_id) return;
  const list = state.inlineApprovals || (state.inlineApprovals = []);
  const existing = list.find((r) => r.approval_id === record.approval_id);
  if (existing) {
    Object.assign(existing, record);
  } else {
    list.push({ ...record });
  }
  // 卡片一旦出现即滚动到消息区底部，保证用户立刻看到
  scrollMessagesToBottom();
}

/* ====================================================================
   【高危审批交互修复】把后端 pending 审批记录同步进内联卡片区并重绘。

   历史缺陷：审批记录只落在 state.approvals（审批标签页数据源），
   而内联卡片只读 state.inlineApprovals；而 inlineApprovals 仅在
   "POST /api/session/chat 的响应体里带 pending_approval" 时才会被注入。
   由于后端返回 waiting_approval 时前端已进入 afterTask（state.running=false），
   且 SSE 的 approval 事件只更新任务面板、从不拉取审批记录 ——
   结果：底部输入框上方永远不出现【同意 / 拒绝】按钮，审批流程无法交互。
   修复：任何"进入等待审批"的路径都统一调用本函数拉取并注入。
   ==================================================================== */
async function syncPendingApprovals() {
  await loadApprovals();                       /* 拉取后端权威审批记录 */
  const sid = state.currentSessionId;
  (state.approvals || []).forEach((r) => {
    if (!r || r.state !== 'pending') return;
    const sameSession = !sid || !r.session_id || r.session_id === sid;
    if (!sameSession) return;
    trackInlineApproval(r);
  });
  /* 【需求点 Bug2】存在 pending 审批 → 同步进入阻塞态（输入框上方按钮 + 拦截提示） */
  syncApprovalBlockFromRecords();
  if (!(state.approvals || []).some((r) => r && r.state === 'pending')) exitApprovalBlock();
  renderInlineApproval();                      /* 审批卡片独立于运行态，必须能刷新 */
  renderMessages();
  scrollMessagesToBottom();
}

/* 仅重绘内联审批卡片（不复用整段消息区渲染，保证审批态可见性最高优先级） */
function renderInlineApproval() {
  const wrap = document.getElementById('inlineApprovalWrap');
  const html = renderInlineApprovalCards();
  if (!wrap) return;
  if (!html) { wrap.remove(); return; }
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  const fresh = tmp.firstElementChild;
  if (fresh) wrap.replaceWith(fresh);
}

/* 【需求点 Bug8】收集应当出现在会话消息区的审批记录：
   1) 当前会话的所有 pending 记录（来自 state.approvals / GET /api/approval/list）
   2) 刚提交任务返回的 pending_approval（已在 trackInlineApproval 注入 state.inlineApprovals）
   3) 本页面会话中已裁决的记录（裁决结果卡片，替代原弹窗的"已提交"反馈）
   按 approval_id 去重，保持原顺序（同组内新记录在后），卡片整体置于消息列表末尾。 */
function collectInlineApprovals() {
  const sid = state.currentSessionId;
  const out = [];
  const byId = {};
  const push = (rec, priority) => {
    if (!rec || !rec.approval_id) return;
    const id = rec.approval_id;
    if (byId[id]) {
      // 同一 approval_id 以「优先级更高」的来源覆盖（pending 状态始终优先展示）
      if (priority < byId[id]._priority) {
        Object.assign(byId[id].record, rec);
        byId[id]._priority = priority;
        byId[id].record._priority = priority;
      }
      return;
    }
    const item = { record: { ...rec, _priority: priority }, _priority: priority };
    byId[id] = item;
    out.push(item.record);
  };

  (state.approvals || []).forEach((r) => {
    const sameSession = !sid || !r.session_id || r.session_id === sid;
    if (!sameSession) return;
    if (r.state === 'pending') push(r, 0);
  });
  (state.inlineApprovals || []).forEach((r) => {
    const sameSession = !sid || !r.session_id || r.session_id === sid;
    if (!sameSession) return;
    if (r.state === 'pending') push(r, 1);
    else if (r.state && state.inlineDecided[r.approval_id]) push(r, 2);
  });
  (state.approvals || []).forEach((r) => {
    if (!state.inlineDecided[r.approval_id]) return;
    const sameSession = !sid || !r.session_id || r.session_id === sid;
    if (!sameSession) return;
    push(r, 2);
  });

  out.forEach((r) => { delete r._priority; });
  return out;
}

/* 【需求点 Bug8】取触发该审批的 Agent 实际运行的模型名称（后端真实绑定快照） */
function approvalModelLabel(record) {
  if (!record) return '';
  if (record.model_label) return String(record.model_label);
  const role = record.agent_role || '';
  const found = (state.agentModels || []).find((m) => m.agent === role);
  if (!found) return '';
  return String(found.actual_model_label || found.assigned_model_label || found.model_label || '');
}

/* ====================================================================
   【第三轮·Bug1】两阶段计时（严格按新需求，前端只展示、绝不自行判定）
   --------------------------------------------------------------------
   阶段 A：高危操作命中 → waiting_approval → 等待用户点击【✅ 执行一次】/【❌ 拒绝】
           → **没有 30 秒超时限制**（resume_state='idle'），不显示倒计时；
   阶段 B：用户点击按钮 → POST /api/approval/submit 到达后端
           → 后端写入 resume_deadline=now+30s（resume_state='running'）
           → 前端显示"执行链路计时 30 秒"倒计时；
   结果  ：链路在 30 秒内收敛 → resume_state='settled'（计时关闭）；
           30 秒未收敛 → 后端判定超时 → **任务失败**（approval_timeout 事件）。
   ==================================================================== */
const APPROVAL_BUSY_STATUSES = ['resuming', 'running', 'waiting_approval', 'pending'];
const APPROVAL_DONE_STATUSES = ['success', 'finished', 'failed', 'cancelled'];

/* 该审批是否处于"提交后执行链路计时中"（只有此时才显示倒计时） */
function approvalCounting(record) {
  if (!record) return false;
  if (record.resume_state) return record.resume_state === 'running';
  return Number(record.remaining_seconds || 0) > 0;
}

/* 剩余秒数：优先用后端给的 remaining_seconds（真值来源），
   其次由 resume_deadline 本地推算（仅用于两次轮询之间的平滑展示） */
function approvalRemaining(record) {
  if (!approvalCounting(record)) return null;
  const remaining = record.remaining_seconds;
  if (remaining != null) return Math.max(0, Number(remaining));
  const deadline = Number(record.resume_deadline || 0);
  if (!deadline) return null;
  return Math.max(0, deadline - Date.now() / 1000);
}

/* 把某条"提交后计时中"的审批登记进全局倒计时表（可能同时存在多条） */
function watchApprovalDeadline(record) {
  if (!record || !record.approval_id) return;
  if (!approvalCounting(record)) { clearApprovalDeadline(record.approval_id); return; }
  const remaining = approvalRemaining(record);
  if (remaining == null) return;
  if (!state.approvalDeadlines) state.approvalDeadlines = {};
  state.approvalDeadlines[String(record.approval_id)] = Date.now() / 1000 + remaining;
  ensureApprovalCountdownTicker();
}

function clearApprovalDeadline(approvalId) {
  if (state.approvalDeadlines) delete state.approvalDeadlines[String(approvalId || '')];
}

function ensureApprovalCountdownTicker() {
  if (state.approvalCountdownTimer) return;
  state.approvalCountdownTimer = setInterval(() => {
    let alive = false;
    (state.approvals || []).concat(state.inlineApprovals || []).forEach((r) => {
      if (!r || !approvalCounting(r)) return;
      const deadline = (state.approvalDeadlines || {})[String(r.approval_id)];
      if (!deadline) return;
      const remaining = Math.max(0, deadline - Date.now() / 1000);
      alive = true;
      const el = document.querySelector(
        `[data-approval-countdown="${String(r.approval_id)}"]`);
      if (!el) return;
      el.textContent = remaining > 0
        ? `⏱ 执行链路计时 ${Math.ceil(remaining)} 秒（提交后 30 秒未完成 → 任务失败）`
        : '⏱ 执行链路已超过 30 秒，后端正在判定任务失败…';
      el.classList.toggle('expired', remaining <= 0);
    });
    if (!alive) stopApprovalCountdownTicker();
  }, 500);
}

function stopApprovalCountdownTicker() {
  if (state.approvalCountdownTimer) {
    clearInterval(state.approvalCountdownTimer);
    state.approvalCountdownTimer = null;
  }
}

/* ====================================================================
   【第三轮·Bug2 修复】提交审批后的收口轮询
   --------------------------------------------------------------------
   历史缺陷（前端停在"等待审批"）：原实现只看 /api/approval/status 的
   task_status 与 resume_in_progress，缺少以下出口，导致状态一旦停在
   waiting_approval 就永远不收敛：
     · 审批已终态但大任务未更新（快照残留）→ 无出口；
     · 本条恢复过程中又产生**新的**待审批单（多次审批场景）→ 无识别；
     · 执行链路超时（任务失败）→ 只当普通失败，卡片文案不更新；
     · 后端受限 30 秒，前端却允许轮询 180 秒 → 超时后空转。
   现在：本函数以"提交后 30 秒窗口"为硬上限，并且四个出口全部显式处理。
   ==================================================================== */
async function pollApprovalUntilSettled(approvalId, info = null) {
  const id = String(approvalId || '');
  const started = Date.now();
  /* 提交后执行链路的硬上限：后端 30 秒 + 5 秒网络/调度余量 */
  const limitMs = Math.max(
    15000,
    Number((info || {}).timeout_seconds || 30) * 1000 + 5000);
  let consecutiveErrors = 0;
  while (Date.now() - started < limitMs) {
    let st = null;
    try {
      st = await api('/api/approval/status/' + encodeURIComponent(id));
      consecutiveErrors = 0;
    } catch (e) {
      consecutiveErrors += 1;
      if (consecutiveErrors >= 5) return null;      /* 后端不可达 → 退出，避免无限轮询 */
    }
    if (st) {
      const taskStatus = String(st.task_status || '');
      const resuming = Boolean(st.resume_in_progress);

      /* 【硬性约束1】每轮用后端权威数据刷新聊天区（只替换为后端已有消息，不清空） */
      await refreshSessionData();

      /* 出口①：执行链路超时 → 任务失败（后端已把子任务+大任务标记 failed） */
      if (st.resume_timed_out || String(st.resume_state) === 'timeout') {
        clearApprovalDeadline(id);
        state.pendingResumes[id] = false;
        toast('审批已提交，但执行链路 30 秒未完成 → 任务判定失败');
        await refreshStatus();
        return st;
      }
      /* 出口②：本条恢复过程中又出现新的待审批单（多次审批场景）→ 交接给新卡片 */
      const sid = state.currentSessionId;
      const pending = (state.approvals || []).filter((r) => r && r.state === 'pending'
        && (!sid || !r.session_id || r.session_id === sid));
      if (pending.length) {
        state.pendingResumes[id] = false;
        clearApprovalDeadline(id);
        renderMessages();                 /* 新审批卡片已在 syncPendingApprovals 中注入 */
        return st;
      }
      /* 出口③：任务已收敛（success/finished/failed）且不再有恢复在途 → 正常结束 */
      if (APPROVAL_DONE_STATUSES.includes(taskStatus) && !resuming) {
        state.pendingResumes[id] = false;
        clearApprovalDeadline(id);
        await refreshStatus();
        return st;
      }
      /* 出口④：任务状态既非终态也不在运行态（数据不一致）→ 再刷一次面板后退出 */
      if (!APPROVAL_BUSY_STATUSES.includes(taskStatus) && !resuming) {
        state.pendingResumes[id] = false;
        await refreshTaskPanel();
        return st;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1200));
  }
  /* 轮询窗口耗尽：不再静默退出，明确提示并刷新一次，避免界面"卡死"观感 */
  state.pendingResumes[id] = false;
  clearApprovalDeadline(id);
  toast('该审批的恢复进度查询已超时，请查看任务面板或重试');
  await refreshTaskPanel();
  return null;
}

/* 【需求点 Bug8】风险等级样式类：high / mid / low */
function riskClass(level) {
  const v = String(level || 'high').toLowerCase();
  if (v === 'low' || v === 'mid' || v === 'high') return v;
  if (v === 'medium' || v === 'middle') return 'mid';
  return 'high';
}

/* 【需求点 Bug8】单条内联审批卡片 HTML（全部字段经 escapeHtml，长文本不截断） */
function renderInlineApprovalCard(record) {
  if (!record || !record.approval_id) return '';
  const id = String(record.approval_id);
  const isPending = record.state === 'pending';
  const busy = state.inlineApprovalBusy === id;
  const errMsg = state.inlineApprovalErrors[id] || '';
  const modelLabel = approvalModelLabel(record);
  const level = String(record.risk_level || 'high').toUpperCase();
  const decidedText = record.state_label || INLINE_DECIDED_LABEL[record.state] || record.state || '';

  // 【需求点 Bug8】操作参数：原样展示原始命令/路径，可滚动，绝不截断
  const paramsHtml = record.operation_params
    ? `<div class="inline-approval-params-label">执行参数</div>
       <div class="inline-approval-params">${escapeHtml(String(record.operation_params))}</div>`
    : `<div class="inline-approval-row"><span class="iar-label">执行参数</span><span class="iar-value">（无）</span></div>`;

  /* 【第三轮·Bug1】两种完全不同的阶段文案：
     · 等待用户点击（resume_state='idle'）→ 无超时限制，不显示倒计时；
     · 审批已提交（resume_state='running'）→ 显示"执行链路计时 30 秒"倒计时。 */
  const counting = isPending && approvalCounting(record);
  const remaining = counting ? approvalRemaining(record) : null;
  const expired = remaining != null && remaining <= 0;
  const resumeState = String(record.resume_state || 'idle');
  if (isPending) watchApprovalDeadline(record);
  let countdownHtml = '';
  if (isPending && counting) {
    countdownHtml = `<div class="inline-approval-countdown${expired ? ' expired' : ''}"
            data-approval-countdown="${escapeHtml(id)}">${
         expired ? '⏱ 执行链路已超过 30 秒，后端正在判定任务失败…'
                 : `⏱ 执行链路计时 ${Math.ceil(remaining == null ? 30 : remaining)} 秒（提交后 30 秒未完成 → 任务失败）`}</div>`;
  } else if (isPending) {
    countdownHtml = '<div class="inline-approval-countdown idle" data-approval-idle="1">'
      + '🕒 等待你点击按钮（本阶段<b>没有</b>超时限制，可安心核对操作内容）</div>';
  }
  const actionsHtml = isPending
    ? `${countdownHtml}
       <div class="inline-approval-actions">
         <button type="button" class="inline-approval-btn reject" data-tip="拒绝：本次高危操作不执行，当前子任务标记失败并回传队长" data-inline-approval-reject="${escapeHtml(id)}"${busy ? ' disabled' : ''}>${busy ? '提交中…' : '❌ 拒绝'}</button>
         <button type="button" class="inline-approval-btn confirm" data-tip="执行一次：仅本次放行该高危操作，执行结果回传队长继续业务循环" data-inline-approval-approve="${escapeHtml(id)}"${busy ? ' disabled' : ''}>${busy ? '提交中…' : '✅ 执行一次'}</button>
       </div>`
    : `<div class="inline-approval-decided ${escapeHtml(record.state || '')}">${escapeHtml(decidedText)}</div>`
      + (resumeState === 'timeout'
         ? '<div class="inline-approval-error">⏱ 审批已提交，但后续 Agent 执行链路 30 秒未完成 → 任务判定失败（高危操作未执行）</div>'
         : '');

  const errHtml = errMsg
    ? `<div class="inline-approval-error">⛔ ${escapeHtml(errMsg)}</div>`
    : '';

  return `
    <div class="inline-approval-card ${isPending ? 'pending' : 'decided'}" data-approval-card="${escapeHtml(id)}">
      <div class="inline-approval-head">
        <span class="inline-approval-title">⚠️ 高危操作待人工审批</span>
        <span class="risk-tag ${riskClass(record.risk_level)}">${escapeHtml(level)}</span>
      </div>
      <div class="inline-approval-body">
        <div class="inline-approval-row"><span class="iar-label">风险等级</span><span class="iar-value"><span class="risk-tag ${riskClass(record.risk_level)}">${escapeHtml(level)}</span></span></div>
        <div class="inline-approval-row"><span class="iar-label">操作类型</span><span class="iar-value">${escapeHtml(record.operation_type || '高危操作')}</span></div>
        <div class="inline-approval-row"><span class="iar-label">执行 Agent</span><span class="iar-value">${escapeHtml(record.agent_role || '—')}</span></div>
        ${modelLabel ? `<div class="inline-approval-row"><span class="iar-label">触发模型</span><span class="iar-value">${escapeHtml(modelLabel)}</span></div>` : ''}
        <div class="inline-approval-row"><span class="iar-label">操作描述</span><span class="iar-value iar-desc">${escapeHtml(record.operation_desc || '（无操作描述）')}</span></div>
        ${paramsHtml}
        <div class="inline-approval-row"><span class="iar-label">风险说明</span><span class="iar-value iar-desc danger">${escapeHtml(record.danger_reason || '（后端未提供风险说明）')}</span></div>
      </div>
      <div class="inline-approval-note">审批结果提交后由后端二次校验（指纹 / 风险等级 / 会话一致性），前端无法绕过，也不会在本机执行任何操作。</div>
      ${errHtml}
      ${actionsHtml}
    </div>`;
}

/* 【需求点 Bug8】消息区末尾的内联审批卡片集合（位于输入框正上方，最新一条） */
function renderInlineApprovalCards() {
  const records = collectInlineApprovals();
  if (!records.length) return '';
  const cards = records.map((r) => renderInlineApprovalCard(r)).filter(Boolean).join('');
  if (!cards) return '';
  return `
    <div class="inline-approval-wrap" id="inlineApprovalWrap">
      ${cards}
    </div>`;
}

/* 【需求点 Bug8】内联审批卡片按钮点击：置「提交中」→ 复用既有 submitApproval 提交（不重复发 HTTP）
   → 重新渲染卡片（成功显示裁决状态，失败常驻显示后端中文原因）。前端不执行任何操作。 */
async function handleInlineApprovalClick(approvalId, approved) {
  const id = String(approvalId || '');
  if (!id) return;
  if (state.inlineApprovalBusy) { toast('上一条审批正在提交，请稍候'); return; }
  state.inlineApprovalBusy = id;
  delete state.inlineApprovalErrors[id];
  renderMessages();                       /* 立即进入「提交中…」并禁用两个按钮 */

  try {
    await submitApproval(id, Boolean(approved));
    if (approved) markInlineDecided(id, 'manual'); else markInlineDecided(id, 'rejected');
    delete state.inlineApprovalErrors[id];
    /* 【需求点 Bug2】审批裁决完成 → 退出阻塞态（队长业务循环已按快照恢复继续跑） */
    exitApprovalBlock();
  } catch (e) {
    /* 【需求点 Bug8】失败原因常驻卡片内（后端中文 message 优先），不使用瞬时可消失的 toast */
    const payload = e && e.payload ? e.payload : {};
    const msg = payload.message || (e && e.message) || '审批提交失败';
    state.inlineApprovalErrors[id] = payload.error ? `${msg}（${payload.error}）` : msg;
  } finally {
    state.inlineApprovalBusy = '';
    renderMessages();
    scrollMessagesToBottom();
    refreshInlineApprovalState(id);
  }
}

/* 【需求点 Bug8】裁决完成后再向后端核对一次状态，保证卡片展示的是后端真实裁决结果 */
async function refreshInlineApprovalState(approvalId) {
  try {
    await loadApprovals();
    renderMessages();
    scrollMessagesToBottom();
  } catch (e) { /* 状态核对失败不打断交互，下次刷新时纠正 */ }
}

/* 【需求点 Bug8】清空内联审批卡片的本地状态（切换/新建会话、回到欢迎首页时调用） */
function resetInlineApprovalTracking() {
  state.inlineApprovals = [];
  state.inlineDecided = {};
  state.inlineApprovalErrors = {};
  state.inlineApprovalBusy = '';
  /* 【新增】清空 30 秒审批倒计时登记，避免残留定时器 */
  state.approvalDeadlines = {};
  stopApprovalCountdownTicker();
  /* 【需求点 Bug2】新会话没有历史待审批 → 同步清空阻塞态 */
  resetApprovalBlock();
}

/* 【需求点 Bug8】标记本页面会话内已裁决的审批（用于继续展示裁决结果卡片） */
function markInlineDecided(approvalId, stateName) {
  if (!approvalId) return;
  state.inlineDecided[approvalId] = true;
  const id = String(approvalId);
  const local = (state.inlineApprovals || []).find((r) => r.approval_id === id);
  if (local && local.state === 'pending') local.state = stateName;
  /* 【第三轮·Bug2 修复】把后端权威记录也标记为已裁决：
     原实现只改本地 inlineApprovals，而 loadApprovals() 会用后端记录覆盖展示，
     一旦后端返回的仍是 pending（例如调用顺序竞争），卡片会永久停在"固定中…"。
     这里显式同步一份已裁决状态到 state.approvals，保证渲染数据源一致。 */
  const remote = (state.approvals || []).find((r) => r.approval_id === id);
  if (remote && remote.state === 'pending') remote.state = stateName;
  state.pendingResumes[id] = true;
}

/* ====================================================================
   设置：五大模型 API Key（第7.1 设置功能 / 第8章 /api/config）
   ==================================================================== */
/* ====================================================================
   【需求点 Bug6 / Bug7】设置面板：按厂商分组配置
     · 一个厂商只填写一次 API Key / Base URL；
     · 该厂商下的多个模型标识逐条列出，每条独立「测试连通」；
     · 密钥指纹只展示 SHA256 前 16 位，明文密钥永不返回前端；
     · Qwen 分组内含 Qwen3.8-Max / Qwen3.8-Flash 两个模型标识（【BUG-NEW2】）。
   ==================================================================== */
function providerCard(p, opts = {}) {
  const configured = p.configured;
  const stateCls = configured ? (p.last_test_ok ? 'ok' : 'none') : 'none';
  const stateTxt = configured ? (p.last_test_ok ? '已测试连通' : '已配置未测试') : '未配置';
  const agents = (opts.agents || []).filter((a) => a.provider === p.provider);
  // 【需求点 Bug1】常驻结果：编辑框重新渲染后依然保留，直到用户修改该输入框
  const notice = state.providerNotices[p.provider];
  const noticeHtmlStr = notice ? `<div class="test-result ${notice.ok ? 'ok' : 'bad'}">${notice.html}</div>` : '';
  return `
    <div class="api-key-block" data-provider="${escapeHtml(p.provider)}">
      <div class="api-key-head">
        <span class="api-key-title">${escapeHtml(p.name)}
          <span class="key-state ${stateCls}">${stateTxt}</span>
        </span>
      </div>
      <div class="agent-bind">
        绑定 Agent：${agents.map((a) => `${escapeHtml(a.agent)}（${escapeHtml(a.model_name)}）`).join(' / ') || '—'}
        <br>当前接口模型标识：<b>${escapeHtml(p.model || '')}</b>
        ${configured && p.key_fingerprint ? `<br>密钥指纹：${escapeHtml(p.key_fingerprint)}（SHA256 前16位，明文不出后端）` : ''}
      </div>
      <div class="api-key-row">
        <label>API Key</label>
        <input type="password" id="key-${escapeHtml(p.provider)}" data-edit-provider="${escapeHtml(p.provider)}"
               placeholder="${configured ? escapeHtml(p.masked || '已配置（留空则不修改）') : '请输入 API Key'}">
        <button class="eye-btn" data-eye="${escapeHtml(p.provider)}">👁</button>
      </div>
      <div class="api-key-row">
        <label>Base URL</label>
        <input type="text" id="base-${escapeHtml(p.provider)}" data-edit-provider="${escapeHtml(p.provider)}"
               value="${escapeHtml(p.base_url || '')}">
      </div>
      <div class="api-key-row">
        <label>模型名</label>
        <input type="text" id="model-${escapeHtml(p.provider)}" data-edit-provider="${escapeHtml(p.provider)}"
               value="${escapeHtml(p.model || '')}">
        <button class="test-btn" data-test="${escapeHtml(p.provider)}">测试连通</button>
      </div>
      <div class="test-result ${notice ? (notice.ok ? 'ok' : 'bad') : ''}" id="test-${escapeHtml(p.provider)}">${noticeHtmlStr}</div>
    </div>`;
}

/* ====================================================================
   【需求点 BUG-NEW1】厂商分组卡片 —— 严格按需求布局渲染
     #### <厂商名>
     API Key  [输入框] 👁        ← 眼睛图标切换明文/掩码
     Base URL [输入框]
     模型1    [输入框] [测试连通性]
     模型2    [输入框] [测试连通性]
     [测试该厂商全部模型连通性]
   一个厂商只填写一次 API Key / Base URL，该厂商下多个模型标识逐条可测；
   密钥指纹只在已配置时展示（SHA256 前 16 位），明文密钥永不返回前端。
   ==================================================================== */
function vendorGroupCard(g) {
  const configured = g.configured;
  const stateCls = configured ? (g.last_test_ok ? 'ok' : 'none') : 'none';
  const stateTxt = configured ? (g.last_test_ok ? '已测试连通' : '已配置未测试') : '未配置';
  const pid = escapeHtml(g.provider);
  const models = g.models || [];

  const boundText = (g.bound_agents || [])
    .map((a) => `${escapeHtml(a.agent)}（${escapeHtml(a.model_name)}）`).join('、') || '—';
  const modelIdsText = (g.model_ids || []).map((m) => `<code>${escapeHtml(m)}</code>`).join('、');

  /* 模型行：模型1 / 模型2 …（按需求用中文序号编号，逐条独立测连通） */
  const modelRows = models.map((m, i) => {
    const notice = state.providerNotices[`${g.provider}::${m.model}`];
    const rowState = m.tested_ok
      ? '<span class="key-state ok">已连通</span>'
      : (configured ? '<span class="key-state none">未测试</span>' : '');
    return `
      <div class="api-key-row vendor-model-row" data-vendor-model="${escapeHtml(m.model)}">
        <label>模型${i + 1}</label>
        <input type="text" class="vendor-model-input" data-model-of="${pid}"
               id="model-${pid}-${i}" value="${escapeHtml(m.model)}"
               data-tip="${escapeHtml((m.label || m.model)
                 + (m.bound_agents && m.bound_agents.length
                    ? ` · 绑定：${m.bound_agents.join('、')}` : '')
                 + (m.is_primary ? ' · 主模型' : ''))}"
               placeholder="请输入该厂商下的模型标识">
        ${rowState}
        <button class="test-btn" data-test-model="${pid}" data-model-id="${escapeHtml(m.model)}"
                data-model-index="${i}">测试连通性</button>
      </div>
      <div class="test-result ${notice ? (notice.ok ? 'ok' : 'bad') : ''}"
           id="test-${pid}-${i}">${notice ? notice.html : ''}</div>`;
  }).join('');

  return `
    <div class="api-key-block vendor-group" data-vendor="${pid}">
      <div class="api-key-head">
        <span class="api-key-title">${escapeHtml(g.title || g.name)}
          <span class="key-state ${stateCls}">${stateTxt}</span>
        </span>
      </div>
      <div class="agent-bind">
        绑定Agent：${boundText}
        <br>当前接口模型标识：${modelIdsText || '<b>—</b>'}
        ${configured && g.key_fingerprint
          ? `<br>密钥指纹：<code>${escapeHtml(g.key_fingerprint)}</code>（SHA256 前16位，明文密钥永不返回前端）`
          : ''}
        ${g.description ? `<br><span class="tr-hint">${escapeHtml(g.description)}</span>` : ''}
      </div>
      <div class="api-key-row">
        <label>API Key</label>
        <input type="password" id="key-${pid}" data-edit-provider="${pid}"
               placeholder="${configured ? escapeHtml(g.masked || '已配置（留空则不修改）') : '请输入该厂商 API Key（该厂商下所有模型共用）'}">
        <button class="eye-btn" data-eye="${pid}" data-tip="显示 / 隐藏密钥明文">👁</button>
      </div>
      <div class="api-key-row">
        <label>Base URL</label>
        <input type="text" id="base-${pid}" data-edit-provider="${pid}"
               value="${escapeHtml(g.base_url || '')}">
      </div>
      ${modelRows || '<div class="tr-hint">该厂商暂无可配置的模型标识</div>'}
      <div class="api-key-row vendor-actions">
        <button class="test-btn" data-test-vendor="${pid}">测试连通性</button>
        <span class="tr-hint">密钥只保存一份，逐个模型调用平台接口检测（每次超时 5 秒）</span>
      </div>
      <div class="test-result" id="test-${pid}"></div>
    </div>`;
}

/* 设置面板 / 向导渲染：只渲染后端登记的厂商分组；
   若后端尚未返回分组，则回退到旧的 provider 卡片（兼容旧配置，保证不空白）。 */
function renderVendorOrProviderCards(container, opts = {}) {
  const cfg = state.config || {};
  const groups = cfg.vendor_groups || [];
  if (groups.length) {
    // 【需求点 BUG-NEW1 规则4】只渲染有 Agent 绑定的厂商分组：
    //   Kimi-Flash 零散配置项已下线并收拢进 Kimi 分组，因此不会再出现任何独立输入项。
    const visible = groups.filter((g) => (g.bound_agents || []).length > 0);
    container.innerHTML = visible.map(vendorGroupCard).join('')
      || '<div class="tr-hint">暂无可配置的厂商分组</div>';
    return;
  }
  const providers = cfg.providers || [];
  container.innerHTML = providers.map((p) => providerCard(p, opts)).join('');
}

/* 【需求点 二、Bug2 前后端状态同步】读取后端配置的唯一解析入口。
   后端 /api/config 同时提供三种口径（嵌套 / 扁平 / 别名），此处按优先级解析，
   避免任何一处字段缺失导致前端回退到"默认开启"而与后端真实状态不符。 */
function apiFallbackInfo(cfg) {
  const c = cfg || state.config || {};
  const nested = c.ecosystem_fallback || {};
  return {
    enabled: apiFallbackEnabled(c),
    priority_text: nested.priority_text
      || c.ecosystem_fallback_priority_text
      || 'DeepSeek > Qwen > GLM > Kimi',
    priority: nested.priority || c.ecosystem_fallback_priority || [],
    tested_ok_providers: nested.tested_ok_providers || [],
    description: nested.description || '',
  };
}

function apiFallbackEnabled(cfg) {
  const c = cfg || state.config || {};
  const nested = c.ecosystem_fallback;
  if (nested && typeof nested.enabled === 'boolean') return nested.enabled;
  if (typeof c.model_fallback_enable === 'boolean') return c.model_fallback_enable;
  if (typeof c.ecosystem_fallback_enabled === 'boolean') return c.ecosystem_fallback_enabled;
  // 后端未返回任何口径时才回退默认（仅用于极端异常场景）
  return true;
}

/* 打开设置弹窗：强制从后端拉取最新配置，保证 UI 与后端一致（Bug2 要求 1） */
async function openSettingsModal() {
  try {
    await reloadConfig();          // 拉取后端真实配置（含补位开关）
  } catch (e) {
    /* 配置拉取失败不阻断弹窗打开，用现有状态渲染 */
  }
  state.pendingFallback = null;    // 以服务端值为准
  clearAlert('settings');
  renderApiKeyList();
  openModal('settingsModalOverlay');
}

function renderApiKeyList() {
  const wrap = $('apiKeyList');
  renderVendorOrProviderCards(wrap, { agents: (state.config || {}).agents || [] });
  renderFallbackSwitch();
}

function renderWizard() {
  renderVendorOrProviderCards($('wizardList'), { agents: (state.config || {}).agents || [] });
  updateWizardHint();
}

/* 【需求点 三、4】生态位补位开关渲染 */
function renderFallbackSwitch() {
  const box = $('fallbackSwitchBox');
  if (!box) return;
  box.classList.toggle('hidden', false);

  /* 【需求点 二、Bug2 前后端状态同步】开关状态**只以后端为准**：
     历史 bug —— 前端读 cfg.ecosystem_fallback.enabled，而后端当时只返回扁平字段
     ecosystem_fallback_enabled，取到 undefined 后按 `!== false` 默认渲染成"开启"，
     于是后端已关闭、UI 仍显示开启。现在统一走 apiFallbackEnabled() 解析，
     并在此基础上叠加未保存前的本地乐观状态（pendingFallback）。 */
  const effective = state.pendingFallback !== null
    ? state.pendingFallback
    : apiFallbackEnabled(state.config);

  const eco = apiFallbackInfo(state.config);
  const tested = (eco.tested_ok_providers || []);
  const toggle = $('ecosystemFallbackToggle');
  toggle.checked = effective;

  $('fallbackSwitchDesc').textContent =
    `补位优先级 ${eco.priority_text || 'DeepSeek > Qwen > GLM > Kimi'}`;
  $('fallbackSwitchState').textContent = effective
    ? `当前状态：已开启 · 可用候选模型 ${tested.length} 个` +
      (tested.length ? `（${tested.join(' / ')}）` : '（尚无模型通过连通性测试，补位将无从选择）')
    : '当前状态：已关闭 · 模型失败将直接导致任务失败，不执行跨模型补位';

  // 左下角系统信息同步显示（Bug2 要求"UI / 后端 / 左下角文本"三处一致）
  renderFallbackSysInfo(effective, tested);
}

/* 【需求点 二、Bug2】左下角“生态位补位”提示行：与开关状态、后端配置严格一致 */
function renderFallbackSysInfo(enabled, tested) {
  const el = $('sysInfo');
  if (!el) return;
  const fallbacks = state.ecosystemFallbacks || [];
  let line;
  if (fallbacks.length) {
    line = `生态位补位：${fallbacks.length} 项（${escapeHtml(fallbacks[0].actual_model)}）`;
  } else if (enabled === false) {
    line = '生态位补位：已关闭';
  } else {
    const n = (tested || []).length;
    line = `生态位补位：已开启${n ? ` · 候选 ${n} 个` : ' · 暂无已连通候选'}`;
  }
  const holder = el.querySelector('[data-eco-line]');
  if (holder) {
    holder.innerHTML = line;
  }
}

function updateWizardHint() {
  const cfg = state.config || {};
  const providers = cfg.providers || [];
  const configured = providers.filter((p) => p.configured).length;
  const testedOk = providers.filter((p) => p.configured && p.last_test_ok).length;
  const btn = $('btnWizardFinish');
  btn.disabled = !(configured >= 1 && testedOk >= 1);
  $('wizardHint').textContent =
    `已配置 ${configured} 个模型，已测试连通 ${testedOk} 个 · ` +
    (btn.disabled ? '需至少 1 个模型通过连通性测试才能进入工作台' : '可以进入工作台');
  $('wizardStepLabel').textContent = btn.disabled ? '必填' : '已完成';
  $('wizardStepLabel').style.background = btn.disabled ? 'rgba(242,85,90,0.15)' : 'rgba(76,208,138,0.16)';
  $('wizardStepLabel').style.color = btn.disabled ? 'var(--danger)' : 'var(--success)';
}

async function reloadConfig() {
  const data = await api('/api/config');
  state.config = data.config || {};
  state.config.agents = data.agents || [];
  state.background = state.config.background || { type: 'preset', value: 'night' };
  if (state.background.url && !state.background.value) state.background.value = state.background.url;
  applyBackground();
  renderBgOptions();
  renderModelSelect();
  return data;
}

function renderModelSelect() {
  const sel = $('modelSelect');
  const providers = (state.config && state.config.providers) || [];
  const options = MODEL_OPTIONS.map((m) => {
    const p = providers.find((x) => x.provider === m.id);
    const suffix = p && !p.configured ? '（未配置）' : '';
    return `<option value="${m.id}">${escapeHtml(m.name)}${suffix}</option>`;
  });
  sel.innerHTML = options.join('');
  sel.value = state.modelHint || 'auto';
}

/* 【需求点 Bug6】收集当前面板上所有「厂商分组」的密钥 / BaseURL / 模型标识清单。
   同一个厂商只提交一次密钥 → 后端只保存一份（AES-256-GCM 加密 + SHA256 篡改校验）。 */
function collectVendorPayload() {
  const groups = (state.config && state.config.vendor_groups) || [];
  const api_keys = {};
  const base_urls = {};
  const vendor_models = {};   // provider -> [model, ...]
  groups.forEach((g) => {
    const keyEl = $(`key-${g.provider}`);
    if (keyEl && keyEl.value.trim()) api_keys[providerKey(g.provider)] = keyEl.value.trim();
    const baseEl = $(`base-${g.provider}`);
    if (baseEl && baseEl.value.trim()) base_urls[g.provider] = baseEl.value.trim();
    const models = [];
    document.querySelectorAll(`.vendor-model-input[data-model-of="${g.provider}"]`).forEach((el) => {
      const v = (el.value || '').trim();
      if (v && !models.includes(v)) models.push(v);
    });
    if (models.length) vendor_models[g.provider] = models;
  });
  return { api_keys, base_urls, vendor_models };
}

/* 回退路径（后端未返回 vendor_groups 时）：沿用旧的单 provider 表单收集 */
function collectProviderPayload() {
  const providers = (state.config && state.config.providers) || [];
  const api_keys = {}, base_urls = {}, models = {};
  providers.forEach((p) => {
    const keyEl = $(`key-${p.provider}`);
    if (keyEl && keyEl.value.trim()) api_keys[providerKey(p.provider)] = keyEl.value.trim();
    const baseEl = $(`base-${p.provider}`);
    if (baseEl && baseEl.value.trim()) base_urls[p.provider] = baseEl.value.trim();
    const modelEl = $(`model-${p.provider}`);
    if (modelEl && modelEl.value.trim()) models[p.provider] = modelEl.value.trim();
  });
  return { api_keys, base_urls, models };
}

async function saveKeys() {
  const hasVendors = Boolean((state.config && state.config.vendor_groups || []).length);
  const payload = hasVendors ? collectVendorPayload() : collectProviderPayload();

  // 【需求点 Bug1】保存失败 → 常驻错误提示（不自动消失），完整展示后端错误文本
  clearAlert('settings');
  const btn = $('btnSaveKeys');
  btn.disabled = true;
  btn.textContent = '保存中…';
  try {
    // 1) 保存密钥 / BaseURL（厂商级，一次性）
    const data = await api('/api/config', {
      method: 'POST',
      body: {
        api_keys: payload.api_keys,
        base_urls: payload.base_urls,
        models: payload.models || {},
      },
    });
    // 2) 【需求点 BUG-NEW1 / BUG-NEW2】逐厂商保存模型标识清单（Qwen3.8-Max / Qwen3.8-Flash 等）
    const modelNotes = [];
    for (const [provider, models] of Object.entries(payload.vendor_models || {})) {
      try {
        const vm = await api('/api/config/vendor-models', {
          method: 'POST', body: { provider, models },
        });
        if ((vm.warnings || []).length) modelNotes.push(`${provider}：${vm.warnings.join('；')}`);
      } catch (e) {
        const p = e.payload || {};
        setAlert('settings', {
          level: 'error',
          title: `${provider} 模型标识保存失败`,
          code: p.error || `HTTP_${e.status || 0}`,
          message: p.message || e.message || '模型标识未通过校验',
          hint: p.hint || '请核对模型标识是否为该平台实际可用的名称。',
        });
        return;
      }
    }

    const saved = data.saved || [];
    await reloadConfig();
    renderApiKeyList();
    if (saved.length || modelNotes.length) {
      setAlert('settings', {
        level: 'ok',
        title: '配置已保存',
        message: (saved.length
          ? `已更新 ${saved.length} 个厂商密钥：${saved.map((s) => vendorTitle(s)).join('、')}`
          : '未填写新的密钥（仅更新模型标识清单）')
          + '（AES-256-GCM 加密 + SHA256 校验，已回读确认落盘）'
          + (modelNotes.length ? `\n模型标识提示：${modelNotes.join(' | ')}` : ''),
        hint: '同一厂商下多个 Agent 共用这一份密钥；建议对每个模型点击「测试连通」确认可用。',
      });
      toast(`配置已保存：${saved.length} 个厂商密钥已加密落盘`);
    } else {
      setAlert('settings', {
        level: 'warn',
        title: '没有需要保存的内容',
        message: '所有 API Key 输入框均为空。若只想修改 Base URL 或模型标识，请确认已填写对应输入框。',
      });
    }
  } catch (e) {
    setAlert('settings', {
      level: 'error',
      title: '配置保存失败',
      code: e.code || `HTTP_${e.status || 0}`,
      message: e.payload && e.payload.message ? e.payload.message : (e.message || '未知错误'),
      hint: (e.payload && e.payload.hint) || '请检查后端服务是否运行、config 目录是否可写后重试。',
      raw: e.payload ? JSON.stringify(e.payload, null, 2) : '',
    });
  } finally {
    btn.disabled = false;
    btn.textContent = '保存配置';
  }
}

/* provider id → 厂商展示名（用于提示文案） */
function vendorTitle(provider) {
  const g = ((state.config && state.config.vendor_groups) || [])
    .find((x) => x.provider === provider);
  return g ? (g.title || g.name) : provider;
}

/* provider id 可能含短横线，这里统一取实际输入框的 provider（保持原样即可） */
function providerKey(provider) { return provider; }

/* 【需求点 Bug6 规则3 / Bug7】逐模型连通性测试（同一厂商密钥，逐个模型验证） */
async function testVendorModel(provider, modelId, scope = 'settings', opts = {}) {
  const keyEl = $(`key-${provider}`);
  const baseEl = $(`base-${provider}`);
  const typedKey = keyEl && keyEl.value.trim() ? keyEl.value.trim() : '';
  const outId = opts.all ? `test-${provider}` : `test-${provider}-${opts.index}`;
  const outEl = $(outId);
  const btnSelector = opts.all
    ? `[data-test-vendor="${provider}"]`
    : `[data-test-model="${provider}"][data-model-index="${opts.index}"]`;
  const btn = document.querySelector(btnSelector);
  const idleText = opts.all ? '测试该厂商全部模型连通性' : '测试连通';

  if (btn) { btn.disabled = true; btn.textContent = '测试中…'; }
  if (outEl) { outEl.className = 'test-result'; outEl.innerHTML = '<span class="tr-hint">正在测试（每个模型后端超时限制 5 秒）…</span>'; }

  try {
    const data = await api('/api/config/test-model', {
      method: 'POST',
      body: {
        provider,
        api_key: typedKey || null,
        base_url: baseEl ? baseEl.value.trim() : null,
        models: opts.all ? collectVendorModelsFromForm(provider) : [modelId],
        persist: Boolean(typedKey),
      },
    });
    const results = data.results || [];
    results.forEach((r) => {
      const key = `${provider}::${r.model}`;
      state.providerNotices[key] = { ok: Boolean(r.ok), html: noticeHtml(r) };
      document.querySelectorAll(`.vendor-model-input[data-model-of="${provider}"]`).forEach((el, i) => {
        if (el.value.trim() !== r.model) return;
        const cell = $(`test-${provider}-${i}`);
        if (cell) { cell.className = 'test-result ' + (r.ok ? 'ok' : 'bad'); cell.innerHTML = noticeHtml(r); }
      });
    });
    if (outEl) {
      const passCount = results.filter((r) => r.ok).length;
      outEl.className = 'test-result ' + (passCount ? 'ok' : 'bad');
      outEl.innerHTML = `<div class="tr-title">${escapeHtml(data.message || '')}</div>`
        + results.map((r) => `<div class="tr-line">${r.ok ? '✅' : '⛔'} <b>${escapeHtml(r.model)}</b>：${escapeHtml(r.message || '')}</div>`).join('');
    }

    await reloadConfig();
    if (scope === 'wizard') { renderWizard(); } else { renderApiKeyList(); }

    if (data.ok) { clearAlert(scope); } else {
      const first = results.find((r) => !r.ok) || {};
      setAlert(scope, {
        level: 'error',
        title: `${data.name || vendorTitle(provider)} 连通性测试未全部通过`,
        code: first.error_code || 'MODEL_TEST_FAILED',
        message: first.message || data.message || '未知错误',
        hint: first.hint || '该厂商下所有模型共用同一密钥，请核对密钥、Base URL 与模型标识。',
        raw: first.raw_detail || '',
      });
    }
    return Boolean(data.ok);
  } catch (e) {
    const payload = e.payload || {};
    const html = noticeHtml({
      ok: false,
      message: payload.message || e.message || '请求失败',
      error_code: payload.error || payload.error_code || `HTTP_${e.status || 0}`,
      error_category: payload.error_category || '请求失败',
      hint: payload.hint || '请确认后端服务已启动（默认 127.0.0.1:5090）且该厂商配置正确。',
      http_status: e.status || 0, elapsed_ms: 0,
    });
    if (outEl) { outEl.className = 'test-result bad'; outEl.innerHTML = html; }
    setAlert(scope, {
      level: 'error',
      title: `${vendorTitle(provider)} 连通性测试请求失败`,
      code: payload.error || `HTTP_${e.status || 0}`,
      message: payload.message || e.message || '请求失败',
      hint: payload.hint || '请确认后端服务已启动（默认 127.0.0.1:5090）。',
    });
    return false;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = idleText; }
  }
}

/* 读取当前表单里该厂商的全部模型标识（用于「测试全部模型」） */
function collectVendorModelsFromForm(provider) {
  const out = [];
  document.querySelectorAll(`.vendor-model-input[data-model-of="${provider}"]`).forEach((el) => {
    const v = (el.value || '').trim();
    if (v && !out.includes(v)) out.push(v);
  });
  return out;
}

async function testProvider(provider, scope = 'settings') {
  const keyEl = $(`key-${provider}`);
  const baseEl = $(`base-${provider}`);
  const modelEl = $(`model-${provider}`);
  const outEl = $(`test-${provider}`);
  const btn = document.querySelector(`[data-test="${provider}"]`);
  const typedKey = keyEl && keyEl.value.trim() ? keyEl.value.trim() : '';

  if (btn) { btn.disabled = true; btn.textContent = '测试中…'; }
  if (outEl) { outEl.className = 'test-result'; outEl.innerHTML = '<span class="tr-hint">正在测试（后端超时限制 5 秒）…</span>'; }

  try {
    const data = await api('/api/config/test-key', {
      method: 'POST',
      body: {
        provider,
        api_key: typedKey || null,
        base_url: baseEl ? baseEl.value.trim() : null,
        model: modelEl ? modelEl.value.trim() : null,
        persist: Boolean(typedKey),
      },
    });
    const html = noticeHtml(data);
    // 【需求点 Bug1】测试结果常驻该模型卡片内，切换/重渲染后仍保留，直到修改输入框
    state.providerNotices[provider] = { ok: Boolean(data.ok), html };
    if (outEl) { outEl.className = 'test-result ' + (data.ok ? 'ok' : 'bad'); outEl.innerHTML = html; }
    await reloadConfig();
    if (scope === 'wizard') { renderWizard(); } else { renderApiKeyList(); }
    if (!data.ok) {
      // 同时给出常驻顶部提示，避免用户只看卡片内小字
      setAlert(scope, {
        level: 'error',
        title: `${data.name || provider} 连通性测试失败`,
        code: data.error_code || '',
        message: data.message || '未知错误',
        hint: data.hint || '',
        raw: data.raw_detail || '',
      });
    } else {
      clearAlert(scope);
    }
    return data.ok;
  } catch (e) {
    const payload = e.payload || {};
    const html = noticeHtml({
      ok: false,
      message: payload.message || e.message || '请求失败',
      error_code: payload.error || payload.error_code || `HTTP_${e.status || 0}`,
      error_category: payload.error_category || '请求失败',
      hint: payload.hint || '请确认后端服务已启动（默认 127.0.0.1:5090）且该模型配置正确。',
      http_status: e.status || 0,
      elapsed_ms: 0,
    });
    state.providerNotices[provider] = { ok: false, html };
    if (outEl) { outEl.className = 'test-result bad'; outEl.innerHTML = html; }
    setAlert(scope, {
      level: 'error',
      title: `${provider} 连通性测试请求失败`,
      code: payload.error || `HTTP_${e.status || 0}`,
      message: payload.message || e.message || '请求失败',
      hint: payload.hint || '请确认后端服务已启动（默认 127.0.0.1:5090）。',
    });
    return false;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '测试连通'; }
  }
}

/* 【需求点 三、4 / 二、Bug2】切换生态位补位开关
   流程（保证前端 UI、后端内存配置、持久化 json、左下角提示四者一致）：
     1. 立即按用户点击渲染（乐观更新，避免"点了没反应"）
     2. POST /api/config 提交新值，后端落盘并回传**最新完整配置**
     3. 用后端返回值覆盖本地 state.config 并重新渲染（以后端为唯一真相源）
     4. 刷新 /api/status，使左下角提示与状态栏同步
     5. 提交失败 → 回滚 UI 到后端原值并给出常驻错误提示
*/
async function toggleEcosystemFallback(enabled) {
  const toggle = $('ecosystemFallbackToggle');
  const previous = apiFallbackEnabled(state.config);
  const next = Boolean(enabled);

  // 1) 乐观更新 UI
  state.pendingFallback = next;
  toggle.checked = next;
  toggle.disabled = true;
  renderFallbackSwitch();
  clearAlert('settings');

  try {
    const data = await api('/api/config', {
      method: 'POST', body: { ecosystem_fallback_enabled: next },
    });
    // 3) 以后端返回值为准（后端返回的是保存后的完整配置）
    state.config = data.config || state.config;
    const confirmed = apiFallbackEnabled(state.config);
    state.pendingFallback = null;
    toggle.checked = confirmed;
    renderFallbackSwitch();
    // 4) 同步左下角系统信息 / 状态栏
    await loadStatus();

    const eco = apiFallbackInfo(state.config);
    setAlert('settings', {
      level: 'ok',
      title: confirmed ? '已开启模型生态位自动补位' : '已关闭模型生态位自动补位',
      message: confirmed
        ? `专属模型不可用时会按 ${eco.priority_text} 自动补齐生态位。`
        : '关闭后模型失败将直接导致任务失败，不执行跨模型补位。',
      hint: `后端已保存：model_fallback_enable = ${confirmed}`,
    });
  } catch (e) {
    // 5) 回滚到后端原值
    state.pendingFallback = previous;
    toggle.checked = previous;
    renderFallbackSwitch();
    const payload = e.payload || {};
    setAlert('settings', {
      level: 'error', title: '开关保存失败', code: payload.error || e.code || `HTTP_${e.status || 0}`,
      message: payload.message || e.message || '未知错误',
      hint: '请检查后端服务状态后重试；UI 已回滚为后端当前值。',
    });
  } finally {
    state.pendingFallback = null;
    toggle.disabled = false;
  }
}

async function finishWizard() {
  clearAlert('wizard');
  try {
    const data = await api('/api/config', { method: 'POST', body: { complete_first_launch: true } });
    state.config = data.config || state.config;
    await reloadConfig();
    closeModal('wizardOverlay');
    toast('配置向导已完成，进入工作台');
    await bootWorkspace();
  } catch (e) {
    const payload = e.payload || {};
    setAlert('wizard', {
      level: 'error',
      title: '配置向导未通过',
      code: payload.error || `HTTP_${e.status || 0}`,
      message: payload.message || e.message || '请至少配置一个模型并完成连通性测试',
      hint: payload.error === 'WIZARD_INCOMPLETE'
        ? '规则：至少一个模型已填写 API Key，且至少一个已配置模型点击「测试连通」通过。'
        : '请检查各模型配置项后重试。',
    });
  }
}

/* ====================================================================
   Session 日志抽屉
   ==================================================================== */
async function showSessionLog() {
  const body = $('logBody');
  body.textContent = '加载中…';
  openModal('logModalOverlay');
  try {
    const data = await api('/api/session/list');
    const cur = (data.sessions || []).find((s) => s.session_id === state.currentSessionId);
    const status = state.status || {};
    const lines = [
      `会话 ID   : ${state.currentSessionId || '—'}`,
      `标题      : ${cur ? cur.title : '—'}`,
      `工作目录  : ${cur ? cur.dir : '—'}`,
      `任务数    : ${(state.tasks || []).length}`,
      `消息数    : ${(state.messages || []).length}`,
      `数据根目录: ${(status.root || '—')}`,
      '',
      '— 消息总线 —',
      JSON.stringify(status.bus || {}, null, 2),
      '',
      '— Agent_Router —',
      JSON.stringify(status.router || {}, null, 2),
      '',
      '— 审批中心 —',
      JSON.stringify(status.approval || {}, null, 2),
      '',
      '— 向量库（长期记忆，90天TTL）—',
      JSON.stringify(status.vector_db || {}, null, 2),
      '',
      '— 记忆管理Agent —',
      JSON.stringify(status.memory || {}, null, 2),
      '',
      '— 本会话 Token 统计（后端真实采集）—',
      JSON.stringify(status.session_tokens || {}, null, 2),
      '',
      '— 任务列表 —',
      ...(state.tasks || []).map((t) =>
        `${(t.task_id || '').slice(0, 8)}  ${STATUS_LABEL[t.status] || t.status}  ${t.agent_role}  迭代${t.iteration}  ${t.title}`),
    ];
    body.textContent = lines.join('\n');
  } catch (e) {
    body.textContent = `加载失败：${e.message}`;
  }
}

/* ====================================================================
   渲染汇总 & 事件绑定
   ==================================================================== */
function renderAll() {
  renderTasks();
  renderMessages();
  renderApprovals();
  renderStatusBar();
  renderMainArea();
  /* 【需求点 二、任务执行计时器】统一在整体重绘时刷新计时组件 */
  renderTaskTimer();
}

function switchTab(tab) {
  state.activeTab = tab;
  document.querySelectorAll('.tab-btn').forEach((btn) => btn.classList.toggle('active', btn.dataset.tab === tab));
  $('chatTab').classList.toggle('hidden', tab !== 'chat');
  $('approvalTab').classList.toggle('hidden', tab !== 'approval');
  updateTabIndicator();
  if (tab === 'approval') loadApprovals();
}

function updateTabIndicator() {
  const active = document.querySelector('.tab-btn.active');
  const tabs = $('tabs');
  const ind = $('tabIndicator');
  if (!active) return;
  const tr = tabs.getBoundingClientRect();
  const br = active.getBoundingClientRect();
  ind.style.left = (br.left - tr.left) + 'px';
  ind.style.width = br.width + 'px';
}

function bindEvents() {
  $('btnNewSession').addEventListener('click', () => newSession());
  /* 【需求点 二、2-c】终止任务：停止后端计时 + 收敛任务状态 */
  $('btnCancelTask').addEventListener('click', cancelRunningTask);
  $('searchInput').addEventListener('input', () => loadWorkspaces());
  /* 【需求点 Bug10】「视图选项」按钮：点击弹出下拉菜单（分组方式 / 排序方式） */
  $('btnViewOptions').addEventListener('click', (e) => {
    e.stopPropagation();
    toggleViewOptions();
  });
  $('viewOptionsMenu').addEventListener('click', (e) => {
    e.stopPropagation();
    const item = e.target.closest('.vo-item');
    if (!item) return;
    if (item.dataset.groupMode !== undefined) setViewOption('group', item.dataset.groupMode);
    else if (item.dataset.sortMode !== undefined) setViewOption('sort', item.dataset.sortMode);
  });
  // 点击菜单外部自动收起
  document.addEventListener('click', (e) => {
    if (e.target.closest('#viewOptionsMenu') || e.target.closest('#btnViewOptions')) return;
    toggleViewOptions(false);
  });
  window.addEventListener('keydown', (e) => { if (e.key === 'Escape') toggleViewOptions(false); });

  /* 【需求点 二、3】新建工作区 */
  $('btnNewWorkspace').addEventListener('click', createWorkspace);
  $('btnWsRenameConfirm').addEventListener('click', confirmWorkspaceRename);
  $('wsRenameInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') confirmWorkspaceRename(); });
  $('btnWsDeleteCancel').addEventListener('click', () => closeModal('wsDeleteOverlay'));
  $('btnWsDeleteConfirm').addEventListener('click', confirmWorkspaceDelete);

  /* 左侧工作区树：展开/折叠、选中工作区、选中会话、工作区与会话操作 */
  $('workspaceList').addEventListener('click', async (e) => {
    const wsNew = e.target.closest('[data-ws-new]');
    if (wsNew) { e.stopPropagation(); await newSession(wsNew.dataset.wsNew); return; }
    const wsRename = e.target.closest('[data-ws-rename]');
    if (wsRename) { e.stopPropagation(); openWorkspaceRename(wsRename.dataset.wsRename); return; }
    const wsDelete = e.target.closest('[data-ws-delete]');
    if (wsDelete) { e.stopPropagation(); openWorkspaceDelete(wsDelete.dataset.wsDelete); return; }
    const moveBtn = e.target.closest('[data-move-session]');
    if (moveBtn) { e.stopPropagation(); openSessionMove(moveBtn.dataset.moveSession); return; }

    const item = e.target.closest('[data-session]');
    if (item) { await selectSession(item.dataset.session); return; }

    const head = e.target.closest('[data-ws-head]');
    if (head) {
      const id = head.dataset.wsHead;
      // 点击已选中的工作区 → 折叠/展开；点击其他工作区 → 切换上下文
      if (id === state.currentWorkspaceId) {
        state.collapsedWorkspaces[id] = !state.collapsedWorkspaces[id];
      } else {
        state.collapsedWorkspaces[id] = false;
        await selectWorkspace(id);
        return;
      }
      renderWorkspaces();
    }
  });

  /* 【需求点 二、3】会话移动归属 */
  $('sessionMoveList').addEventListener('click', async (e) => {
    const target = e.target.closest('[data-move-to]');
    if (!target) return;
    if (target.classList.contains('current')) { toast('该会话已在此工作区'); return; }
    await confirmSessionMove(target.dataset.moveSessionId, target.dataset.moveTo);
  });

  /* 【需求点 二、1】工作区选择下拉按钮 */
  $('btnWsPicker').addEventListener('click', (e) => { e.stopPropagation(); toggleWsPicker(); });
  $('wsPickerMenu').addEventListener('click', async (e) => {
    const add = e.target.closest('[data-pick-add]');
    if (add) { await addWorkspaceFolder(); return; }
    const item = e.target.closest('[data-pick-ws]');
    if (!item) return;
    toggleWsPicker(false);
    const id = item.dataset.pickWs;
    if (id === state.currentWorkspaceId) return;
    await selectWorkspace(id);
    await refreshWorkspaceRoot();
    toast(`已切换工作区：${(currentWorkspace() || {}).name || ''}`);
  });
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.ws-picker')) toggleWsPicker(false);
  });

  /* 【需求点 二、1】目录路径兜底弹窗 + 候选选择 */
  $('btnFolderPathConfirm').addEventListener('click', async () => {
    const path = ($('folderPathInput').value || '').trim();
    if (!path) { toast('请输入文件夹完整路径'); return; }
    await registerFolderPath(path);
  });
  $('folderPathInput').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') $('btnFolderPathConfirm').click();
  });
  $('folderPickList').addEventListener('click', async (e) => {
    const item = e.target.closest('[data-pick-path]');
    if (!item) return;
    await registerFolderPath(item.dataset.pickPath);
  });

  /* 【需求点 Bug1】常驻错误提示：手动关闭 */
  document.addEventListener('click', (e) => {
    const closeBtn = e.target.closest('[data-alert-close]');
    if (closeBtn) {
      const scope = closeBtn.dataset.alertClose;
      const current = state.alerts[scope];
      if (current && current.timer) clearTimeout(current.timer);
      state.alerts[scope] = null;
      renderAlert(scope);
    }
  });

  /* 【需求点 三、2】生态位补位轻提示关闭 */
  $('ecoNoticeClose').addEventListener('click', hideEcoNotice);

  /* 【需求点 Bug1】修改任一输入框内容 → 清除该模型与顶部的错误提示 */
  document.addEventListener('input', (e) => {
    const target = e.target;
    if (!target || !target.dataset) return;
    const provider = target.dataset.editProvider;
    if (provider) {
      clearProviderNotice(provider);
      // 用户已修改输入 → 立即清除提示（标记 dirty，绕过最小展示时长）
      const scope = target.closest('#wizardOverlay') ? 'wizard' : 'settings';
      const current = state.alerts[scope];
      if (current) {
        current.dirty = true;
        if (current.timer) clearTimeout(current.timer);
        state.alerts[scope] = null;
        renderAlert(scope);
      }
    }
  });
  /* 【需求点 三、4】生态位补位开关 */
  $('ecosystemFallbackToggle').addEventListener('change', (e) => {
    toggleEcosystemFallback(e.target.checked);
  });

  $('btnBackground').addEventListener('click', () => { renderBgOptions(); openModal('bgModalOverlay'); });
  /* 【需求点 二、Bug2】打开设置面板前先拉取后端真实配置，避免 UI 与后端状态不同步 */
  $('btnSettings').addEventListener('click', () => { openSettingsModal(); });
  $('btnSessionLog').addEventListener('click', showSessionLog);

  document.querySelectorAll('.modal-close').forEach((btn) => {
    btn.addEventListener('click', () => closeModal(btn.dataset.close));
  });
  document.querySelectorAll('.modal-overlay').forEach((overlay) => {
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay && overlay.id !== 'wizardOverlay') {
        overlay.classList.add('hidden');
      }
    });
  });

  /* 背景选择 */
  $('bgOptions').addEventListener('click', async (e) => {
    const opt = e.target.closest('.bg-option[data-bg]');
    if (!opt) return;
    state.background = { type: 'preset', value: opt.dataset.bg };
    applyBackground(); renderBgOptions();
    try { await api('/api/config', { method: 'POST', body: { background: state.background } }); } catch (err) { /* 忽略 */ }
    toast('背景已切换');
  });
  $('bgUploadOption').addEventListener('click', () => $('bgFileInput').click());
  $('bgFileInput').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const fd = new FormData();
    fd.append('file', file);
    try {
      const data = await api('/api/background/upload', { method: 'POST', body: fd });
      state.background = data.background;
      applyBackground(); renderBgOptions();
      toast(`自定义背景已应用（后端安全校验通过，${(data.size / 1024).toFixed(0)}KB）`);
    } catch (err) {
      toast(`背景上传失败：${err.message}`);
    } finally {
      e.target.value = '';
    }
  });

  /* 设置弹窗内交互（事件委托，兼容向导与设置两个容器） */
  document.addEventListener('click', async (e) => {
    const eye = e.target.closest('[data-eye]');
    if (eye) {
      const input = $(`key-${eye.dataset.eye}`);
      if (!input) return;
      const showing = input.type === 'text';
      input.type = showing ? 'password' : 'text';
      eye.textContent = showing ? '👁' : '🙈';
      return;
    }
    const testBtn = e.target.closest('[data-test]');
    if (testBtn) {
      const inWizard = Boolean(e.target.closest('#wizardOverlay'));
      await testProvider(testBtn.dataset.test, inWizard ? 'wizard' : 'settings');
      return;
    }
    /* 【需求点 Bug6 规则3 / Bug7】厂商分组：逐模型「测试连通」 */
    const modelTestBtn = e.target.closest('[data-test-model]');
    if (modelTestBtn) {
      const inWizard = Boolean(e.target.closest('#wizardOverlay'));
      await testVendorModel(
        modelTestBtn.dataset.testModel,
        modelTestBtn.dataset.modelId,
        inWizard ? 'wizard' : 'settings',
        { index: modelTestBtn.dataset.modelIndex },
      );
      return;
    }
    /* 【需求点 Bug6 规则3】厂商分组：一次性测试该厂商下全部模型 */
    const vendorTestBtn = e.target.closest('[data-test-vendor]');
    if (vendorTestBtn) {
      const inWizard = Boolean(e.target.closest('#wizardOverlay'));
      await testVendorModel(
        vendorTestBtn.dataset.testVendor, '', inWizard ? 'wizard' : 'settings', { all: true },
      );
      return;
    }
  });

  /* 【需求点 Bug3】全局错误兜底：任何未捕获异常 / 未处理的 Promise 拒绝
     一律只写浏览器控制台，**绝不调用 alert/confirm 等原生弹窗** */
  window.addEventListener('error', (e) => {
    console.warn('[全局错误捕获] 已记录到控制台，不弹出任何浏览器对话框', {
      message: e.message, source: e.filename, line: e.lineno, col: e.colno, error: e.error,
    });
  });
  window.addEventListener('unhandledrejection', (e) => {
    console.warn('[未处理的 Promise 拒绝] 已记录到控制台，不弹出任何浏览器对话框', e.reason);
  });

  $('btnSaveKeys').addEventListener('click', saveKeys);
  $('btnWizardFinish').addEventListener('click', finishWizard);

  /* 标签切换 */
  document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });

  /* 任务面板展开收起 */
  $('taskHeader').addEventListener('click', () => {
    taskPanelCollapsed = !taskPanelCollapsed;
    $('taskList').classList.toggle('collapsed', taskPanelCollapsed);
    $('taskToggle').classList.toggle('collapsed', taskPanelCollapsed);
  });

  /* 消息区：思考链折叠（第7.3 可折叠）+ 【需求点 Bug8】内联高危审批卡片按钮 */
  $('messages').addEventListener('click', (e) => {
    /* 【需求点 Bug8】内联审批卡片：拒绝 / 执行一次 → 仅提交结果，由后端二次校验裁决 */
    const rejectBtn = e.target.closest('[data-inline-approval-reject]');
    if (rejectBtn) { handleInlineApprovalClick(rejectBtn.dataset.inlineApprovalReject, false); return; }
    const approveBtn = e.target.closest('[data-inline-approval-approve]');
    if (approveBtn) { handleInlineApprovalClick(approveBtn.dataset.inlineApprovalApprove, true); return; }

    const header = e.target.closest('[data-toggle-thinking]');
    if (!header) return;
    const chain = header.nextElementSibling;
    const chevron = header.querySelector('.chevron');
    if (chain) chain.classList.toggle('collapsed');
    if (chevron) chevron.classList.toggle('collapsed');
    /* 【需求点 Bug2】记住用户对"运行中思维链"的折叠选择，
       避免新流式事件触发重绘时被强制展开（历史思维链不受影响） */
    if (header.closest('#liveMsg')) {
      state.liveChainCollapsed = chain ? chain.classList.contains('collapsed') : false;
    }
  });

  /* 审批面板操作 */
  $('approvalList').addEventListener('click', (e) => {
    const ok = e.target.closest('[data-approve]');
    if (ok) { submitApproval(ok.dataset.approve, true); return; }
    const no = e.target.closest('[data-reject]');
    if (no) { submitApproval(no.dataset.reject, false); }
  });
  $('btnApprovalScope').addEventListener('click', () => {
    state.approvalScope = state.approvalScope === 'session' ? 'all' : 'session';
    loadApprovals();
  });
  $('btnApprovalClear').addEventListener('click', async () => {
    if (!(state.approvals || []).length) return;
    /* 【需求点 Bug3】禁止浏览器原生 confirm 弹窗：改为按钮二次确认（3 秒内再点一次即执行） */
    const btn = $('btnApprovalClear');
    if (btn.dataset.armed !== '1') {
      btn.dataset.armed = '1';
      const original = btn.textContent;
      btn.textContent = '再点一次确认清空';
      setTimeout(() => {
        if (btn.dataset.armed === '1') {
          btn.dataset.armed = '0';
          btn.textContent = original || '清空全部';
        }
      }, 3000);
      toast('确认清空审批记录？3 秒内再点一次执行（存在待审批项时后端会拒绝）');
      return;
    }
    btn.dataset.armed = '0';
    btn.textContent = '清空全部';
    try {
      const q = state.currentSessionId ? `?session_id=${encodeURIComponent(state.currentSessionId)}` : '';
      const data = await api('/api/approval/clear' + q, { method: 'POST' });
      toast(`已清空 ${data.removed} 条审批记录`);
      await loadApprovals();
    } catch (e) {
      toast(`清空失败：${e.message}`);
    }
  });

  /* 输入区 */
  const chatInput = $('chatInput');
  chatInput.addEventListener('input', () => {
    chatInput.style.height = 'auto';
    chatInput.style.height = Math.min(chatInput.scrollHeight, 120) + 'px';
  });
  chatInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }  /* Enter 发送 */
    /* Shift+Enter 换行：不拦截 */
  });
  $('btnSend').addEventListener('click', () => sendMessage());
  $('modelSelect').addEventListener('change', () => {
    state.modelHint = $('modelSelect').value;
    renderStatusBar();
  });

  /* + 号菜单 */
  const plusMenu = $('plusMenu');
  $('btnPlus').addEventListener('click', (e) => { e.stopPropagation(); plusMenu.classList.toggle('hidden'); });
  document.addEventListener('click', (e) => { if (!e.target.closest('.plus-wrap')) plusMenu.classList.add('hidden'); });
  plusMenu.addEventListener('click', (e) => {
    const action = e.target.closest('button')?.dataset.action;
    if (!action) return;
    plusMenu.classList.add('hidden');
    if (action === 'upload') $('chatFileInput').click();
    else if (action === 'workspace') { chatInput.value += (chatInput.value ? ' ' : '') + '@工作区 '; chatInput.focus(); }
    else if (action === 'command') { chatInput.value += (chatInput.value ? ' ' : '') + '/'; chatInput.focus(); }
  });
  $('chatFileInput').addEventListener('change', async (e) => {
    await handleFiles(e.target.files);
    e.target.value = '';
  });
  $('attachStrip').addEventListener('click', (e) => {
    const btn = e.target.closest('[data-remove]');
    if (!btn) return;
    state.attachments.splice(Number(btn.dataset.remove), 1);
    renderAttachments();
  });

  /* 拖拽上传 */
  document.addEventListener('dragover', (e) => e.preventDefault());
  document.addEventListener('drop', async (e) => {
    if (e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length) {
      e.preventDefault();
      await handleFiles(e.dataTransfer.files);
    }
  });

  /* 登录 */
  $('btnLogin').addEventListener('click', async () => {
    clearAlert('login');
    try {
      const data = await api('/api/auth/login', {
        method: 'POST',
        body: { username: $('loginUser').value, password: $('loginPass').value },
      });
      state.auth.authenticated = true;
      state.auth.username = data.username;
      closeModal('loginOverlay');
      toast('登录成功');
      await bootWorkspace();
    } catch (e) {
      setAlert('login', {
        level: 'error',
        title: '登录失败',
        code: e.code || `HTTP_${e.status || 0}`,
        message: (e.payload && e.payload.message) || e.message || '用户名或密码错误',
        hint: '初始口令为 admin123，首次登录后请尽快修改（bcrypt 哈希存储）。',
      });
      toast('登录失败，请查看弹窗内错误说明');
    }
  });
  $('loginPass').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('btnLogin').click(); });

  /* 【需求点 二、1】欢迎首页大输入框：直接输入需求 → 自动创建会话并进入对话 */
  const welcomeInput = $('welcomeInput');
  welcomeInput.addEventListener('input', () => {
    welcomeInput.style.height = 'auto';
    welcomeInput.style.height = Math.min(welcomeInput.scrollHeight, 200) + 'px';
  });
  welcomeInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendFromWelcome();
    }
  });
  $('btnWelcomeSend').addEventListener('click', sendFromWelcome);

  window.addEventListener('resize', updateTabIndicator);
}

/* 欢迎首页发送：自动创建会话并切入对话界面 */
async function sendFromWelcome() {
  const input = $('welcomeInput');
  const text = (input.value || '').trim();
  if (!text) { toast('请输入需求内容'); return; }
  if (state.running) { toast('当前任务仍在执行，请等待完成'); return; }
  input.value = '';
  input.style.height = 'auto';
  await sendMessage(text);
}

/* ====================================================================
   启动流程（第7.4 首次启动强制流程）
   ==================================================================== */
async function bootWorkspace() {
  await reloadConfig();
  try {
    await loadWorkspaces();
  } catch (e) { /* 未认证时静默，登录后重试 */ }
  // 【需求点 二、1/2】启动时默认不选中任何会话 → 直接展示欢迎首页（不预生成会话）
  state.currentSessionId = null;
  state.tasks = []; state.messages = []; state.approvals = [];
  renderMainArea();
  await refreshStatus();
  renderAll();
  /* 【需求点 Bug9】启动底部统计栏 3 秒轮询（真实后端数据） */
  startStatsPolling();

  if (!state.config.first_launch_completed) {
    // 无有效配置 → 强制弹出 API 配置向导，不可跳过
    clearAlert('wizard');
    renderWizard();
    openModal('wizardOverlay');
    toast('首次启动：请完成模型 API 配置向导');
  }
}

async function init() {
  bindEvents();
  /* 【增量修复】初始化黑底白字自定义 Tooltip（替换浏览器原生浅色提示） */
  initTooltips();
  applyBackground();
  /* 【需求点 Bug10】恢复用户上次选择的视图偏好（分组方式 / 排序方式） */
  loadViewPrefs();
  /* 【需求点 二、2】初始状态：没有正在运行的任务 → 计时器显示「未开始任务」 */
  resetTimerState();
  renderAll();
  requestAnimationFrame(updateTabIndicator);

  /* 1. 认证状态（第6章 6.3 局域网访问强制登录） */
  try {
    const auth = await api('/api/auth/status');
    state.auth.authenticated = auth.authenticated;
    state.auth.localBypass = auth.local_bypass;
  } catch (e) { /* 忽略 */ }

  const config = await (async () => {
    try { return await api('/api/config'); } catch (e) { return null; }
  })();

  if (!config) {
    // /api/config 需要认证（局域网访问强制登录）→ 弹出登录框
    openModal('loginOverlay');
    return;
  }
  await bootWorkspace();
}

document.addEventListener('DOMContentLoaded', init);

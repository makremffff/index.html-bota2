// /api/index.js
// Full backend with ADMIN_USER_ID hardcoded for admin-only endpoints (bypass action_id).
// NOTE: This file includes an embedded ADMIN_USER_ID (trusted single admin).
// Replace 7741750541 with your actual Telegram admin user id if different.

const crypto = require('crypto');

// Load environment variables for Supabase connection
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
// ⚠️ BOT_TOKEN must be set in Vercel environment variables to use Telegram checks
const BOT_TOKEN = process.env.BOT_TOKEN;

// ----------------------------
// Hardcoded admin id (per request)
const ADMIN_USER_ID = 7741750541; // <-- Put your admin Telegram id here
// ----------------------------

// Server constants
const REWARD_PER_AD = 3;
const REFERRAL_COMMISSION_RATE = 0.05;
const DAILY_MAX_ADS = 100;
const DAILY_MAX_SPINS = 15;
const RESET_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 hours
const MIN_TIME_BETWEEN_ACTIONS_MS = 3000; // 3s
const ACTION_ID_EXPIRY_MS = 60000; // 60s
const SPIN_SECTORS = [5, 10, 15, 20, 5];
// NEW: Minimum interval between completing ANY task (especially bot tasks)
const MIN_TASK_COMPLETION_INTERVAL_MS = 5000; // 5 seconds cooldown

const TASK_COMPLETIONS_TABLE = 'user_task_completions';

// Helper functions
function sendSuccess(res, data = {}) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, data }));
}

function sendError(res, message, statusCode = 400) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: false, error: message }));
}

async function supabaseFetch(tableName, method, body = null, queryParams = '?select=*') {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error('Supabase environment variables are not configured.');
  }

  const url = `${SUPABASE_URL}/rest/v1/${tableName}${queryParams}`;
  const headers = {
    'apikey': SUPABASE_ANON_KEY,
    'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };
  const options = { method, headers, body: body ? JSON.stringify(body) : null };

  const response = await fetch(url, options);

  if (response.ok) {
    const text = await response.text();
    try {
      const json = JSON.parse(text);
      return Array.isArray(json) ? json : { success: true };
    } catch (e) {
      return { success: true };
    }
  }

  let data;
  try {
    data = await response.json();
  } catch (e) {
    throw new Error(`Supabase error: ${response.status} ${response.statusText}`);
  }
  throw new Error(data.message || `Supabase error: ${response.status} ${response.statusText}`);
}

function calculateRandomSpinPrize() {
  const randomIndex = Math.floor(Math.random() * SPIN_SECTORS.length);
  const prize = SPIN_SECTORS[randomIndex];
  return { prize, prizeIndex: randomIndex };
}

async function checkChannelMembership(userId, channelUsername) {
  if (!BOT_TOKEN) {
    console.error('BOT_TOKEN not configured.');
    return false;
  }
  const chatId = channelUsername.startsWith('@') ? channelUsername : `@${channelUsername}`;
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/getChatMember?chat_id=${chatId}&user_id=${userId}`;

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      console.error('Telegram API error:', err.description || resp.statusText);
      return false;
    }
    const data = await resp.json();
    if (!data.ok) return false;
    const status = data.result.status;
    return ['member', 'administrator', 'creator'].includes(status);
  } catch (error) {
    console.error('Telegram API fetch error:', error.message);
    return false;
  }
}

function isAdmin(userId) {
  if (!ADMIN_USER_ID) return false;
  const id = parseInt(userId);
  return id === ADMIN_USER_ID;
}

// Action ID system
function generateStrongId() {
  return crypto.randomBytes(32).toString('hex');
}

async function handleGenerateActionId(req, res, body) {
  const { user_id, action_type } = body;
  const id = parseInt(user_id);
  if (!action_type) return sendError(res, 'Missing action_type.', 400);

  try {
    const existing = await supabaseFetch('temp_actions', 'GET', null, `?user_id=eq.${id}&action_type=eq.${action_type}&select=action_id,created_at`);
    if (Array.isArray(existing) && existing.length > 0) {
      const lastTime = new Date(existing[0].created_at).getTime();
      if (Date.now() - lastTime < ACTION_ID_EXPIRY_MS) {
        return sendSuccess(res, { action_id: existing[0].action_id });
      } else {
        await supabaseFetch('temp_actions', 'DELETE', null, `?user_id=eq.${id}&action_type=eq.${action_type}`);
      }
    }
  } catch (e) {
    console.warn('temp_actions check error:', e.message);
  }

  const newActionId = generateStrongId();
  try {
    await supabaseFetch('temp_actions', 'POST', { user_id: id, action_id: newActionId, action_type }, '?select=action_id');
    sendSuccess(res, { action_id: newActionId });
  } catch (error) {
    console.error('Failed to save action id:', error.message);
    sendError(res, 'Failed to generate security token.', 500);
  }
}

async function validateAndUseActionId(res, userId, actionId, actionType) {
  if (!actionId) {
    sendError(res, 'Missing Server Token (Action ID). Request rejected.', 400);
    return false;
  }
  try {
    const query = `?user_id=eq.${userId}&action_id=eq.${actionId}&action_type=eq.${actionType}&select=id,created_at`;
    const records = await supabaseFetch('temp_actions', 'GET', null, query);
    if (!Array.isArray(records) || records.length === 0) {
      sendError(res, 'Invalid or previously used Server Token (Action ID).', 409);
      return false;
    }
    const rec = records[0];
    const recTime = new Date(rec.created_at).getTime();
    if (Date.now() - recTime > ACTION_ID_EXPIRY_MS) {
      await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${rec.id}`);
      sendError(res, 'Server Token (Action ID) expired. Please try again.', 408);
      return false;
    }
    await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${rec.id}`);
    return true;
  } catch (error) {
    console.error('validateAndUseActionId error:', error.message);
    sendError(res, 'Security validation failed.', 500);
    return false;
  }
}

// validate initData from Telegram WebApp (unchanged)
function validateInitData(initData) {
  if (!initData || !BOT_TOKEN) return false;

  const urlParams = new URLSearchParams(initData);
  const hash = urlParams.get('hash');
  urlParams.delete('hash');

  const dataCheckString = Array.from(urlParams.entries())
    .map(([k, v]) => `${k}=${v}`)
    .sort()
    .join('\n');

  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const calculatedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

  if (calculatedHash !== hash) return false;

  const authDateParam = urlParams.get('auth_date');
  if (!authDateParam) return false;
  const authDate = parseInt(authDateParam) * 1000;
  if (Date.now() - authDate > 20 * 60 * 1000) return false;
  return true;
}

// Placeholder for processCommission and resetDailyLimitsIfExpired (ensure these are present in the full backend implementation)
async function processCommission(referrerId, completedUserId, reward) {
  // Implement commission processing logic here (e.g., fetch referrer, calculate commission, update referrer balance, log transaction)
}

async function resetDailyLimitsIfExpired(userId) {
  // Implement logic to reset ads_watched_today and spins_today based on RESET_INTERVAL_MS
}

async function checkRateLimit(userId) {
  // Implement logic to check MIN_TIME_BETWEEN_ACTIONS_MS
  return { ok: true }; // Placeholder for successful check
}

// --- API Handlers ---

async function handleGetUserData(req, res, body) {
  const { user_id } = body;
  if (!user_id) return sendError(res, 'Missing user_id for data fetch.');
  const id = parseInt(user_id);

  try {
    await resetDailyLimitsIfExpired(id);
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,ads_watched_today,spins_today,is_banned,ref_by,ads_limit_reached_at,spins_limit_reached_at,task_completed`);
    if (!users || users.length === 0 || users.success) {
      return sendSuccess(res, { balance: 0, ads_watched_today: 0, spins_today: 0, referrals_count: 0, withdrawal_history: [], is_banned: false, task_completed: false });
    }
    const userData = users[0];
    if (userData.is_banned) return sendSuccess(res, { is_banned: true, message: "User is banned from accessing the app." });

    const referrals = await supabaseFetch('users', 'GET', null, `?ref_by=eq.${id}&select=id`);
    const referralsCount = Array.isArray(referrals) ? referrals.length : 0;
    const history = await supabaseFetch('withdrawals', 'GET', null, `?user_id=eq.${id}&select=amount,status,created_at&order=created_at.desc`);
    const withdrawalHistory = Array.isArray(history) ? history : [];

    await supabaseFetch('users', 'PATCH', { last_activity: new Date().toISOString() }, `?id=eq.${id}&select=id`);

    sendSuccess(res, { ...userData, referrals_count: referralsCount, withdrawal_history: withdrawalHistory });
  } catch (error) {
    console.error('GetUserData failed:', error.message);
    sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
  }
}

async function handleGetTasks(req, res, body) {
  const { user_id } = body;
  const id = parseInt(user_id);
  try {
    // تم تحديث SELECT لجلب حقل 'type'
    const availableTasks = await supabaseFetch('tasks', 'GET', null, `?select=id,name,link,reward,max_participants,type`);
    const completedTasks = await supabaseFetch(TASK_COMPLETIONS_TABLE, 'GET', null, `?user_id=eq.${id}&select=task_id`);
    const completedTaskIds = Array.isArray(completedTasks) ? new Set(completedTasks.map(t => t.task_id)) : new Set();
    const tasksList = Array.isArray(availableTasks) ? availableTasks.map(task => ({
      task_id: task.id, name: task.name, link: task.link, reward: task.reward, max_participants: task.max_participants, is_completed: completedTaskIds.has(task.id), type: task.type || 'channel'
    })) : [];
    sendSuccess(res, { tasks: tasksList });
  } catch (error) {
    console.error('GetTasks failed:', error.message);
    sendError(res, `Failed to retrieve tasks: ${error.message}`, 500);
  }
}

async function handleDeleteTask(req, res, body) {
    const { user_id, action_id, task_id } = body;
    const adminId = parseInt(user_id);
    const taskId = parseInt(task_id);

    if (isNaN(taskId)) return sendError(res, 'Invalid task_id.', 400);

    if (!isAdmin(adminId)) {
        if (!await validateAndUseActionId(res, adminId, action_id, 'deleteTask')) return;
    }

    try {
        await supabaseFetch('tasks', 'DELETE', null, `?id=eq.${taskId}`);
        await supabaseFetch(TASK_COMPLETIONS_TABLE, 'DELETE', null, `?task_id=eq.${taskId}`); // حذف سجلات الإكمال المرتبطة
        sendSuccess(res, { message: `Task ${taskId} and its completions deleted successfully.` });
    } catch (error) {
        console.error('DeleteTask failed:', error.message);
        sendError(res, `Failed to delete task: ${error.message}`, 500);
    }
}

async function handleCreateTask(req, res, body) {
  const { user_id, action_id, name, link, reward, max_participants, note, task_type } = body;
  const id = parseInt(user_id);
  if (!name || !link || (reward === undefined) || isNaN(parseFloat(reward))) return sendError(res, 'Missing required task fields: name, link, reward.', 400);

  if (!isAdmin(id)) {
    if (!await validateAndUseActionId(res, id, action_id, 'createTask')) return;
  }

  try {
    const payload = {
      name: String(name).trim(),
      link: String(link).trim(),
      reward: parseFloat(reward),
      max_participants: (isNaN(parseInt(max_participants)) ? null : parseInt(max_participants)),
      note: note ? String(note).trim() : null,
      created_by: id,
      created_at: new Date().toISOString(),
      type: task_type || 'channel' // استخدام نوع المهمة المُرسل
    };
    await supabaseFetch('tasks', 'POST', payload, '?select=id');
    sendSuccess(res, { message: 'Task created successfully.' });
  } catch (error) {
    console.error('CreateTask failed:', error.message);
    sendError(res, `Failed to create task: ${error.message}`, 500);
  }
}

async function handleSearchUser(req, res, body) {
  const { user_id, action_id, search_user_id } = body;
  const adminId = parseInt(user_id);
  if (!search_user_id) return sendError(res, 'Missing search_user_id.', 400);

  if (!isAdmin(adminId)) {
    if (!await validateAndUseActionId(res, adminId, action_id, 'searchUser')) return;
  }

  try {
    const targetId = parseInt(search_user_id);
    if (isNaN(targetId)) return sendError(res, 'Invalid search_user_id.', 400);
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${targetId}&select=id,first_name,username,balance,ads_watched_today,spins_today,is_banned,ref_by`);
    if (!Array.isArray(users) || users.length === 0) return sendError(res, 'User not found.', 404);
    const u = users[0];
    const userObj = { user_id: u.id, first_name: u.first_name || null, username: u.username || null, balance: u.balance || 0, ads_watched_today: u.ads_watched_today || 0, spins_today: u.spins_today || 0, is_banned: !!u.is_banned, ref_by: u.ref_by || null };
    sendSuccess(res, { user: userObj });
  } catch (error) {
    console.error('SearchUser failed:', error.message);
    sendError(res, `Failed to search user: ${error.message}`, 500);
  }
}

async function handleGetPendingWithdrawals(req, res, body) {
  const { user_id, action_id } = body;
  const adminId = parseInt(user_id);

  if (!isAdmin(adminId)) {
    if (!await validateAndUseActionId(res, adminId, action_id, 'getPendingWithdrawals')) return;
  }

  try {
    const pending = await supabaseFetch('withdrawals', 'GET', null, `?status=eq.pending&select=id,user_id,amount,binance_id,created_at&order=created_at.desc`);
    const pendingList = Array.isArray(pending) ? pending : [];
    sendSuccess(res, { pending_withdrawals: pendingList });
  } catch (error) {
    console.error('GetPendingWithdrawals failed:', error.message);
    sendError(res, `Failed to retrieve pending withdrawals: ${error.message}`, 500);
  }
}

async function handleUpdateBalance(req, res, body) {
  const { user_id, action_id, target_user_id, new_balance } = body;
  const adminId = parseInt(user_id);
  if (!target_user_id) return sendError(res, 'Missing target_user_id.', 400);
  if (new_balance === undefined) return sendError(res, 'Missing new_balance.', 400);

  if (!isAdmin(adminId)) {
    if (!await validateAndUseActionId(res, adminId, action_id, 'updateBalance')) return;
  }

  try {
    const targetId = parseInt(target_user_id);
    const nb = parseFloat(new_balance);
    if (isNaN(nb)) return sendError(res, 'Invalid new_balance value.', 400);
    await supabaseFetch('users', 'PATCH', { balance: nb }, `?id=eq.${targetId}`);
    sendSuccess(res, { message: `Balance updated for user ${targetId}.`, new_balance: nb });
  } catch (error) {
    console.error('UpdateBalance failed:', error.message);
    sendError(res, `Failed to update balance: ${error.message}`, 500);
  }
}

async function handleToggleBan(req, res, body) {
  const { user_id, action_id, target_user_id, action } = body;
  const adminId = parseInt(user_id);
  if (!target_user_id) return sendError(res, 'Missing target_user_id.', 400);
  if (!action) return sendError(res, 'Missing action (ban/unban).', 400);

  if (!isAdmin(adminId)) {
    if (!await validateAndUseActionId(res, adminId, action_id, 'toggleBan')) return;
  }

  try {
    const targetId = parseInt(target_user_id);
    if (isNaN(targetId)) return sendError(res, 'Invalid target_user_id.', 400);
    const setBan = (action === 'ban');
    await supabaseFetch('users', 'PATCH', { is_banned: setBan }, `?id=eq.${targetId}`);
    sendSuccess(res, { message: `User ${targetId} ${setBan ? 'banned' : 'unbanned'}.`, is_banned: setBan });
  } catch (error) {
    console.error('ToggleBan failed:', error.message);
    sendError(res, `Failed to ${action} user: ${error.message}`, 500);
  }
}

// Admin action handler for withdrawing accept/reject/ban
async function handleAdminAction(req, res, body) {
  const { user_id, action_id, action, request_id, user_to_ban } = body;
  const adminId = parseInt(user_id);
  if (!action) return sendError(res, 'Missing action field.', 400);

  if (!isAdmin(adminId)) {
    if (!await validateAndUseActionId(res, adminId, action_id, 'adminAction')) return;
  }

  try {
    if (action === 'ban') {
      if (!user_to_ban) return sendError(res, 'Missing user_to_ban for ban action.', 400);
      const targetId = parseInt(user_to_ban);
      if (isNaN(targetId)) return sendError(res, 'Invalid user_to_ban.', 400);
      await supabaseFetch('users', 'PATCH', { is_banned: true }, `?id=eq.${targetId}`);
      return sendSuccess(res, { message: `User ${targetId} has been banned.` });
    }

    if (!request_id) return sendError(res, 'Missing request_id for withdrawal action.', 400);
    const reqId = parseInt(request_id);
    if (isNaN(reqId)) return sendError(res, 'Invalid request_id.', 400);

    const rows = await supabaseFetch('withdrawals', 'GET', null, `?id=eq.${reqId}&select=id,user_id,amount,status`);
    if (!Array.isArray(rows) || rows.length === 0) return sendError(res, 'Withdrawal request not found.', 404);
    const wr = rows[0];
    if (wr.status !== 'pending') return sendError(res, `Withdrawal is not pending (current status: ${wr.status}).`, 400);

    const targetUserId = wr.user_id;
    const amount = parseFloat(wr.amount) || 0;

    if (action === 'accept') {
      await supabaseFetch('withdrawals', 'PATCH', { status: 'completed' }, `?id=eq.${reqId}`);
      return sendSuccess(res, { message: `Withdrawal ${reqId} accepted (user ${targetUserId}).` });
    }

    if (action === 'reject') {
      const users = await supabaseFetch('users', 'GET', null, `?id=eq.${targetUserId}&select=balance`);
      if (!Array.isArray(users) || users.length === 0) {
        await supabaseFetch('withdrawals', 'PATCH', { status: 'rejected' }, `?id=eq.${reqId}`);
        return sendError(res, 'Target user not found. Withdrawal rejected but refund failed.', 500);
      }
      const user = users[0];
      const newBalance = (parseFloat(user.balance) || 0) + amount;
      await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${targetUserId}`);
      await supabaseFetch('withdrawals', 'PATCH', { status: 'rejected' }, `?id=eq.${reqId}`);
      return sendSuccess(res, { message: `Withdrawal ${reqId} rejected and ${amount} refunded to user ${targetUserId}.`, refunded_amount: amount });
    }

    return sendError(res, `Unknown admin action: ${action}`, 400);
  } catch (error) {
    console.error('AdminAction failed:', error.message);
    return sendError(res, `Failed to perform admin action: ${error.message}`, 500);
  }
}

// Register / Watch / Spin / CompleteTask / Withdraw handlers
async function handleRegister(req, res, body) {
  const { user_id, ref_by } = body;
  const id = parseInt(user_id);
  try {
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned`);
    if (!Array.isArray(users) || users.length === 0) {
      const newUser = { id, balance: 0, ads_watched_today: 0, spins_today: 0, ref_by: ref_by ? parseInt(ref_by) : null, last_activity: new Date().toISOString(), is_banned: false, task_completed: false };
      await supabaseFetch('users', 'POST', newUser, '?select=id');
    } else {
      if (users[0].is_banned) return sendError(res, 'User is banned.', 403);
    }
    sendSuccess(res, { message: 'User registered or already exists.' });
  } catch (error) {
    console.error('Registration failed:', error.message);
    sendError(res, `Registration failed: ${error.message}`, 500);
  }
}

async function handleWatchAd(req, res, body) {
  const { user_id, action_id } = body;
  const id = parseInt(user_id);
  if (!await validateAndUseActionId(res, id, action_id, 'watchAd')) return;
  try {
    await resetDailyLimitsIfExpired(id);
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,ads_watched_today,is_banned,ref_by`);
    if (!Array.isArray(users) || users.length === 0) return sendError(res, 'User not found.', 404);
    const user = users[0];
    if (user.is_banned) return sendError(res, 'User is banned.', 403);
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) return sendError(res, rateLimitResult.message, 429);
    if (user.ads_watched_today >= DAILY_MAX_ADS) return sendError(res, `Daily ad limit (${DAILY_MAX_ADS}) reached.`, 403);

    const reward = REWARD_PER_AD;
    const newBalance = user.balance + reward;
    const newAdsCount = user.ads_watched_today + 1;
    const updatePayload = { balance: newBalance, ads_watched_today: newAdsCount, last_activity: new Date().toISOString() };
    if (newAdsCount >= DAILY_MAX_ADS) updatePayload.ads_limit_reached_at = new Date().toISOString();
    await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);
    if (user.ref_by) processCommission(user.ref_by, id, reward).catch(e => console.error('Commission error:', e.message));
    sendSuccess(res, { new_balance: newBalance, actual_reward: reward, new_ads_count: newAdsCount });
  } catch (error) {
    console.error('WatchAd failed:', error.message);
    sendError(res, `Failed to process ad watch: ${error.message}`, 500);
  }
}

async function handlePreSpin(req, res, body) {
  const { user_id, action_id } = body;
  const id = parseInt(user_id);
  if (!await validateAndUseActionId(res, id, action_id, 'preSpin')) return;
  try {
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=is_banned`);
    if (!Array.isArray(users) || users.length === 0) return sendError(res, 'User not found.', 404);
    if (users[0].is_banned) return sendError(res, 'User is banned.', 403);
    sendSuccess(res, { message: "Pre-spin action secured." });
  } catch (error) {
    console.error('PreSpin failed:', error.message);
    sendError(res, `Failed to secure pre-spin: ${error.message}`, 500);
  }
}

async function handleSpinResult(req, res, body) {
  const { user_id, action_id } = body;
  const id = parseInt(user_id);
  if (!await validateAndUseActionId(res, id, action_id, 'spinResult')) return;
  await resetDailyLimitsIfExpired(id);
  try {
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,spins_today,is_banned`);
    if (!Array.isArray(users) || users.length === 0) return sendError(res, 'User not found.', 404);
    const user = users[0];
    if (user.is_banned) return sendError(res, 'User is banned.', 403);
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) return sendError(res, rateLimitResult.message, 429);
    if (user.spins_today >= DAILY_MAX_SPINS) return sendError(res, `Daily spin limit (${DAILY_MAX_SPINS}) reached.`, 403);

    const { prize, prizeIndex } = calculateRandomSpinPrize();
    const newSpinsCount = user.spins_today + 1;
    const newBalance = user.balance + prize;
    const updatePayload = { balance: newBalance, spins_today: newSpinsCount, last_activity: new Date().toISOString() };
    if (newSpinsCount >= DAILY_MAX_SPINS) updatePayload.spins_limit_reached_at = new Date().toISOString();
    await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);
    await supabaseFetch('spin_results', 'POST', { user_id: id, prize }, '?select=user_id');
    sendSuccess(res, { new_balance: newBalance, actual_prize: prize, prize_index: prizeIndex, new_spins_count: newSpinsCount });
  } catch (error) {
    console.error('Spin result failed:', error.message);
    sendError(res, `Failed to process spin result: ${error.message}`, 500);
  }
}

async function handleCompleteTask(req, res, body) {
  const { user_id, action_id, task_id } = body;
  const id = parseInt(user_id);
  const taskId = parseInt(task_id);
  if (isNaN(taskId)) return sendError(res, 'Missing or invalid task_id.', 400);
  if (!await validateAndUseActionId(res, id, action_id, `completeTask_${taskId}`)) return;

  try {
    // 1. تحقق من الحد الزمني لآخر مهمة مكتملة
    const lastCompletionRecords = await supabaseFetch(TASK_COMPLETIONS_TABLE, 'GET', null, `?user_id=eq.${id}&select=created_at&order=created_at.desc&limit=1`);
    if (Array.isArray(lastCompletionRecords) && lastCompletionRecords.length > 0) {
      const lastCompletionTime = new Date(lastCompletionRecords[0].created_at).getTime();
      if (Date.now() - lastCompletionTime < MIN_TASK_COMPLETION_INTERVAL_MS) {
        return sendError(res, `Please wait ${MIN_TASK_COMPLETION_INTERVAL_MS / 1000} seconds between completing tasks.`, 429);
      }
    }

    // 2. جلب بيانات المهمة
    const tasks = await supabaseFetch('tasks', 'GET', null, `?id=eq.${taskId}&select=link,reward,max_participants,type`);
    if (!Array.isArray(tasks) || tasks.length === 0) return sendError(res, 'Task not found.', 404);
    const task = tasks[0];
    const reward = task.reward;
    const taskLink = task.link;
    const taskType = task.type || 'channel'; // الافتراضي هو 'channel'

    // 3. التحقق مما إذا كانت المهمة قد اكتملت مسبقًا
    const completions = await supabaseFetch(TASK_COMPLETIONS_TABLE, 'GET', null, `?user_id=eq.${id}&task_id=eq.${taskId}&select=id`);
    if (Array.isArray(completions) && completions.length > 0) return sendError(res, 'Task already completed by this user.', 403);

    // 4. التحقق من حدود المعدل العامة وحالة الحظر
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) return sendError(res, rateLimitResult.message, 429);
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,ref_by,is_banned`);
    const user = users[0];
    if (user.is_banned) return sendError(res, 'User is banned.', 403);
    
    // 5. التحقق من الانضمام (يتم تخطيه إذا كان نوع المهمة 'bot' أو 'join_no_check')
    if (taskType === 'channel') { // التعديل هنا: التحقق فقط إذا كان type هو 'channel'
        const channelUsernameMatch = taskLink.match(/t\.me\/([a-zA-Z0-9_]+)/);
        if (!channelUsernameMatch) return sendError(res, 'Task verification failed: The link is not a supported Telegram channel format for join tasks.', 400);
        const channelUsername = `@${channelUsernameMatch[1]}`;
        const isMember = await checkChannelMembership(id, channelUsername);
        if (!isMember) return sendError(res, `User has not joined the required channel: ${channelUsername}`, 400);
    }
    // إذا كان taskType === 'bot'، فسيتم تخطي كتلة التحقق أعلاه.

    // 6. منح الجائزة وتحديث السجلات
    const referrerId = user.ref_by;
    const newBalance = user.balance + reward;
    await supabaseFetch('users', 'PATCH', { balance: newBalance, last_activity: new Date().toISOString() }, `?id=eq.${id}`);
    await supabaseFetch(TASK_COMPLETIONS_TABLE, 'POST', { user_id: id, task_id: taskId, reward_amount: reward }, '?select=user_id');
    if (referrerId) processCommission(referrerId, id, reward).catch(e => console.error('Commission error:', e.message));
    sendSuccess(res, { new_balance: newBalance, actual_reward: reward, message: 'Task completed successfully.' });
  } catch (error) {
    console.error('CompleteTask failed:', error.message);
    sendError(res, `Failed to complete task: ${error.message}`, 500);
  }
}


// Withdraw handler (already defined above as handleWithdraw) - reused
async function handleWithdraw(req, res, body) {
  const { user_id, action_id, amount, binance_id } = body;
  const id = parseInt(user_id);
  if (!amount || !binance_id) return sendError(res, 'Missing withdrawal details (amount/binance_id).', 400);
  if (!await validateAndUseActionId(res, id, action_id, 'withdraw')) return;

  try {
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,is_banned`);
    if (!Array.isArray(users) || users.length === 0) return sendError(res, 'User not found.', 404);
    const user = users[0];
    if (user.is_banned) return sendError(res, 'User is banned.', 403);

    const withdrawalAmount = parseFloat(amount);
    if (isNaN(withdrawalAmount) || withdrawalAmount <= 0) return sendError(res, 'Invalid withdrawal amount.', 400);
    if (withdrawalAmount > user.balance) return sendError(res, 'Insufficient balance.', 400);

    const newBalance = user.balance - withdrawalAmount;
    
    // 1. خصم الرصيد
    await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${id}`);

    // 2. إنشاء طلب سحب معلق
    const withdrawalRecord = {
      user_id: id,
      amount: withdrawalAmount,
      binance_id: String(binance_id).trim(),
      status: 'pending',
      created_at: new Date().toISOString(),
    };
    await supabaseFetch('withdrawals', 'POST', withdrawalRecord, '?select=id');

    sendSuccess(res, { new_balance: newBalance, message: 'Withdrawal request submitted successfully and is pending approval.' });

  } catch (error) {
    console.error('Withdrawal failed:', error.message);
    sendError(res, `Withdrawal failed: ${error.message}`, 500);
  }
}


// Main handler
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return sendSuccess(res);
  if (req.method !== 'POST') return sendError(res, `Method ${req.method} not allowed. Only POST is supported.`, 405);

  let body;
  try {
    body = await new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => data += chunk.toString());
      req.on('end', () => {
        try { resolve(JSON.parse(data)); } catch (e) { reject(new Error('Invalid JSON payload.')); }
      });
      req.on('error', reject);
    });
  } catch (error) {
    return sendError(res, error.message, 400);
  }

  if (!body || !body.type) return sendError(res, 'Missing "type" field in the request body.', 400);

  // Security: require valid initData for most types
  if (body.type !== 'commission' && (!body.initData || !validateInitData(body.initData))) {
    return sendError(res, 'Invalid or expired initData. Security check failed.', 401);
  }

  if (!body.user_id && body.type !== 'commission') return sendError(res, 'Missing user_id in the request body.', 400);

  // Route
  switch (body.type) {
    case 'getUserData': await handleGetUserData(req, res, body); break;
    case 'getTasks': await handleGetTasks(req, res, body); break;
    case 'createTask': await handleCreateTask(req, res, body); break;
    case 'deleteTask': await handleDeleteTask(req, res, body); break;
    case 'searchUser': await handleSearchUser(req, res, body); break;
    case 'getPendingWithdrawals': await handleGetPendingWithdrawals(req, res, body); break;
    case 'updateBalance': await handleUpdateBalance(req, res, body); break;
    case 'toggleBan': await handleToggleBan(req, res, body); break;
    case 'adminAction': await handleAdminAction(req, res, body); break;
    case 'register': await handleRegister(req, res, body); break;
    case 'watchAd': await handleWatchAd(req, res, body); break;
    case 'commission': await handleCommission(req, res, body); break;
    case 'preSpin': await handlePreSpin(req, res, body); break;
    case 'spinResult': await handleSpinResult(req, res, body); break;
    case 'withdraw': await handleWithdraw(req, res, body); break;
    case 'completeTask': await handleCompleteTask(req, res, body); break;
    case 'generateActionId': await handleGenerateActionId(req, res, body); break;
    default: sendError(res, `Unknown request type: ${body.type}`, 400); break;
  }
};
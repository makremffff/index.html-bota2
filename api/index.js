const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

// تهيئة Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseAnonKey);

// الثوابت
const BOT_TOKEN = process.env.BOT_TOKEN;
const MIN_TASK_COMPLETION_INTERVAL_MS = 5000; // 5 seconds cooldown

// ----------------------------------------------------------------------
// الدوال المساعدة
// ----------------------------------------------------------------------

/**
 * إرسال استجابة خطأ موحدة
 * @param {object} res - كائن الاستجابة
 * @param {string} message - رسالة الخطأ
 * @param {number} statusCode - كود حالة HTTP
 */
const sendError = (res, message, statusCode = 500) => {
    res.status(statusCode).json({ success: false, error: message });
};

/**
 * جلب البيانات من Supabase
 * @param {string} table - اسم الجدول
 * @param {string} method - نوع العملية (GET, POST, etc.)
 * @param {object} data - البيانات المراد إرسالها (لعمليات POST/PUT)
 * @param {string} query - سلاسل استعلام إضافية
 */
const supabaseFetch = async (table, method, data = null, query = '') => {
    let request = supabase.from(table);

    if (query) {
        request = request.select(query.replace('?select=', ''));
    } else {
        request = request.select('*');
    }

    if (method === 'POST') {
        request = request.insert(data);
    } else if (method === 'UPDATE') {
        request = request.update(data);
    }

    const { data: result, error } = await request;

    if (error) {
        throw new Error(error.message);
    }
    return result;
};

/**
 * التحقق من عضوية المستخدم في قناة تليجرام
 * @param {string} userId - معرف المستخدم في تليجرام
 * @param {string} channelUsername - اسم المستخدم للقناة (يبدأ بـ @)
 * @returns {Promise<boolean>}
 */
const checkChannelMembership = async (userId, channelUsername) => {
    try {
        const response = await axios.get(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
            params: {
                chat_id: channelUsername,
                user_id: userId
            }
        });

        const status = response.data.result.status;
        return ['member', 'administrator', 'creator'].includes(status);
    } catch (e) {
        console.error('Error checking membership:', e.message);
        return false; // نفترض عدم العضوية في حالة الفشل
    }
};

// ----------------------------------------------------------------------
// معالجات المسارات (Handled Routes)
// ----------------------------------------------------------------------

/**
 * جلب جميع المهام المتاحة
 */
const handleGetTasks = async (req, res) => {
    try {
        // تم التأكد من جلب حقل 'type'
        const availableTasks = await supabaseFetch('tasks', 'GET', null, `?select=id,name,link,reward,max_participants,type`);
        res.status(200).json({ success: true, tasks: availableTasks });
    } catch (e) {
        sendError(res, 'Failed to retrieve tasks.', 500);
    }
};

/**
 * إكمال مهمة والحصول على المكافأة
 */
const handleCompleteTask = async (req, res) => {
    const { id, user_id, task_id } = req.body;

    if (!id || !user_id || !task_id) {
        return sendError(res, 'Missing required fields: id, user_id, or task_id', 400);
    }

    try {
        // 1. جلب بيانات المهمة
        const taskData = await supabaseFetch('tasks', 'GET', null, `?select=id,reward,link,type&id=eq.${task_id}`);
        if (!taskData || taskData.length === 0) {
            return sendError(res, 'Task not found.', 404);
        }
        const { reward, link: taskLink, type: taskType } = taskData[0];

        // 2. التحقق من إكمال المهمة مسبقاً
        const existingCompletion = await supabaseFetch('user_task_completions', 'GET', null, `?select=id&user_id=eq.${user_id}&task_id=eq.${task_id}`);
        if (existingCompletion && existingCompletion.length > 0) {
            return sendError(res, 'Task already completed by this user.', 400);
        }
        
        // 3. التحقق من الحد الزمني الأدنى (5 ثوانٍ بين كل مهمة)
        const lastCompletion = await supabase
            .from('user_task_completions')
            .select('created_at')
            .eq('user_id', user_id)
            .order('created_at', { ascending: false })
            .limit(1);

        if (lastCompletion.data && lastCompletion.data.length > 0) {
            const lastCompletionTime = new Date(lastCompletion.data[0].created_at).getTime();
            const currentTime = Date.now();
            const timeDiff = currentTime - lastCompletionTime;

            if (timeDiff < MIN_TASK_COMPLETION_INTERVAL_MS) {
                return sendError(res, `Please wait ${MIN_TASK_COMPLETION_INTERVAL_MS / 1000} seconds before completing another task.`, 429);
            }
        }

        // 4. التحقق من الانضمام للقناة (يتم **تخطيه بالكامل** إذا كان نوع المهمة 'bot')
        if (taskType !== 'bot') {
            const channelUsernameMatch = taskLink.match(/t\.me\/([a-zA-Z0-9_]+)/);

            if (!channelUsernameMatch) {
                if (taskType === 'channel') {
                    return sendError(res, 'Task verification failed: The link is not a supported Telegram channel format for join tasks.', 400);
                }
            } else {
                const channelUsername = `@${channelUsernameMatch[1]}`;
                const isMember = await checkChannelMembership(user_id, channelUsername);
                if (!isMember) {
                    return sendError(res, `User has not joined the required channel: ${channelUsername}`, 400);
                }
            }
        }
        
        // 5. تحديث رصيد المستخدم ومنح المكافأة
        await supabase.rpc('update_user_balance', {
            p_user_id: user_id,
            p_amount: reward
        });

        // 6. تسجيل إكمال المهمة
        await supabaseFetch('user_task_completions', 'POST', {
            user_id: user_id,
            task_id: task_id
        });

        res.status(200).json({ success: true, message: 'Task completed and reward granted.', reward });

    } catch (e) {
        console.error('Error in handleCompleteTask:', e.message);
        sendError(res, 'An internal error occurred during task completion.', 500);
    }
};

// ----------------------------------------------------------------------
// نقطة الدخول الرئيسية
// ----------------------------------------------------------------------

module.exports = async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    if (req.method === 'OPTIONS') {
        return res.status(204).end();
    }

    try {
        if (req.method === 'GET' && req.url === '/api/tasks') {
            await handleGetTasks(req, res);
        } else if (req.method === 'POST' && req.url === '/api/complete-task') {
            await handleCompleteTask(req, res);
        } else {
            sendError(res, 'Not Found', 404);
        }
    } catch (e) {
        sendError(res, e.message, 500);
    }
};
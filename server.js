require('dotenv').config();
const express  = require('express');
const axios    = require('axios');
const cron     = require('node-cron');
const crypto   = require('crypto');
const { google } = require('googleapis');

const app = express();

// ─── Pipeline Stages ─────────────────────────────────────────────────────────

const STAGES = ['Interested', 'Lead List Ready', 'Provided List'];

// ─── Column Indices (0-based, matching sheet columns A–K) ────────────────────

const COL = {
  EMAIL:           0,  // A
  WEBSITE:         1,  // B
  STAGE:           2,  // C
  STAGE_DATE:      3,  // D
  LAST_REPLY:      4,  // E
  CAMPAIGN:        5,  // F
  AI_REPLY:        6,  // G
  UNIBOX_LINK:     7,  // H
  WHOLESALE_FIELD: 8,  // I — "Wholesale Partner Ideas" custom variable
  TIKTOK_FIELD:    9,  // J — "Top TikTok Shop Seller" custom variable
  REENGAGED:       10, // K — date when added to re-engagement campaign
  // Col L (11) = "Advance Stage" checkbox — managed by Apps Script only
  SCRAPED_CSV_1:   12, // M — Scraped CSV – Partner 1 (first "and"-separated partner idea)
  SCRAPED_CSV_2:   13, // N — Scraped CSV – Partner 2 (second "and"-separated partner idea)
};

// ─── Instantly label IDs → stage name mapping ────────────────────────────────

const LABEL_MAP = () => ({
  'Interested':    process.env.INSTANTLY_LABEL_INTERESTED,
  'Provided List': process.env.INSTANTLY_LABEL_PROVIDED_LIST,
});

// ─── Google Sheets Auth ──────────────────────────────────────────────────────

const SHEET_ID   = process.env.GOOGLE_SHEET_ID;
const SHEET_NAME = 'Master';

// ─── Parse & normalise private key (works with any OpenSSL version) ─────────
const _rawPK = (process.env.GOOGLE_PRIVATE_KEY || '').replace(/\\n/g, '\n');
let _privateKey;
try {
  // Parse → re-export produces a clean PKCS#8 PEM that OpenSSL 3 always accepts
  const keyObj = crypto.createPrivateKey(_rawPK);
  _privateKey = keyObj.export({ type: 'pkcs8', format: 'pem' });
  console.log('[startup] Private key parsed & re-exported OK (length:', _privateKey.length + ')');
} catch (err) {
  console.error('[startup] crypto.createPrivateKey FAILED:', err.message);
  _privateKey = _rawPK; // fallback to raw (will likely fail later)
}

const auth = new google.auth.JWT(
  process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
  null,
  _privateKey,
  ['https://www.googleapis.com/auth/spreadsheets']
);
const sheets = google.sheets({ version: 'v4', auth });

// ─── Middleware ───────────────────────────────────────────────────────────────

// Capture raw body for Slack signature verification before any parsing
app.use((req, res, next) => {
  if (req.path === '/slack/interactions' || req.path === '/slack/scraper-events') {
    let data = '';
    req.on('data', chunk => { data += chunk; });
    req.on('end', () => {
      req.rawBody = data;
      next();
    });
  } else {
    next();
  }
});

app.use('/slack/interactions', express.urlencoded({ extended: true }));
app.use((req, res, next) => {
  if (req.path === '/slack/interactions' || req.path === '/slack/scraper-events') {
    return next(); // raw body already captured above; skip json parser for these paths
  }
  express.json()(req, res, next);
});

// ─── Google Sheets Helpers ───────────────────────────────────────────────────

async function getAllRows() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:N`,
  });
  return (res.data.values || []).slice(1); // skip header row
}

async function findRowByEmail(email) {
  const rows = await getAllRows();
  const idx  = rows.findIndex(r => (r[COL.EMAIL] || '').toLowerCase() === email.toLowerCase());
  if (idx === -1) return null;
  return { rowIndex: idx + 2, rowData: rows[idx] }; // +2 = 1-based + header row
}

async function updateRow(rowIndex, updates) {
  // updates: { colIndex: value, ... }
  const requests = Object.entries(updates).map(([col, value]) => ({
    range: `${SHEET_NAME}!${String.fromCharCode(65 + Number(col))}${rowIndex}`,
    values: [[value]],
  }));
  if (!requests.length) return;
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    resource: {
      valueInputOption: 'USER_ENTERED',
      data: requests,
    },
  });
}

async function appendRow(rowData) {
  // Find the actual last row with data (check col A)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A:A`,
  });
  const lastRow = (res.data.values || []).length + 1;
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A${lastRow}`,
    valueInputOption: 'USER_ENTERED',
    resource: { values: [rowData] },
  });
}

// ─── Instantly API Helpers ───────────────────────────────────────────────────

async function getInstantlyLeadUUID(email) {
  const res = await axios.get('https://api.instantly.ai/api/v2/leads', {
    params: { email, limit: 1 },
    headers: { Authorization: `Bearer ${process.env.INSTANTLY_API_KEY}` },
  });
  const items = res.data.items || res.data || [];
  return items[0]?.id || null;
}

async function updateInstantlyLabel(email, stageName) {
  const labelMap = LABEL_MAP();
  const labelId  = labelMap[stageName];
  if (!labelId) return;
  try {
    const uuid = await getInstantlyLeadUUID(email);
    if (!uuid) return;
    await axios.patch(
      `https://api.instantly.ai/api/v2/leads/${uuid}`,
      { lt_interest_status: labelId },
      { headers: { Authorization: `Bearer ${process.env.INSTANTLY_API_KEY}`, 'Content-Type': 'application/json' } }
    );
  } catch (err) {
    console.error('[instantly] label update failed:', err.response?.data || err.message);
  }
}

async function addToReengagementCampaign(email, website, campaignId) {
  if (!campaignId) { console.error('[reengagement] no campaign ID provided'); return; }
  try {
    await axios.post(
      `https://api.instantly.ai/api/v2/campaigns/${campaignId}/leads`,
      { leads: [{ email, website: website || '' }] },
      { headers: { Authorization: `Bearer ${process.env.INSTANTLY_API_KEY}`, 'Content-Type': 'application/json' } }
    );
    console.log(`[reengagement] added ${email} to campaign ${campaignId}`);
  } catch (err) {
    console.error('[reengagement] failed:', err.response?.data || err.message);
  }
}

async function sendInstantlyReply(email, bodyText) {
  const uuid = await getInstantlyLeadUUID(email);
  if (!uuid) throw new Error(`Lead UUID not found for ${email}`);

  const listRes = await axios.get('https://api.instantly.ai/api/v2/emails', {
    params: { lead_id: uuid, limit: 1 },
    headers: { Authorization: `Bearer ${process.env.INSTANTLY_API_KEY}` },
  });
  const items = listRes.data.items || listRes.data || [];
  const latestEmail = items[0];
  if (!latestEmail) throw new Error(`No emails found for lead ${email}`);

  await axios.post(
    'https://api.instantly.ai/api/v2/emails/reply',
    {
      reply_to_uuid: latestEmail.id,
      eaccount:      latestEmail.eaccount,
      subject:       latestEmail.subject,
      body:          { text: bodyText },
    },
    { headers: { Authorization: `Bearer ${process.env.INSTANTLY_API_KEY}`, 'Content-Type': 'application/json' } }
  );
}

// ─── Pipeline Detection ──────────────────────────────────────────────────────

function getPipelineType(row) {
  if (row[COL.WHOLESALE_FIELD]) return 'wholesale';
  if (row[COL.TIKTOK_FIELD])   return 'tiktok';
  return null;
}

function getReengagementCampaignId(pipelineType) {
  if (pipelineType === 'wholesale') return process.env.INSTANTLY_WHOLESALE_REENGAGEMENT_CAMPAIGN_ID;
  if (pipelineType === 'tiktok')   return process.env.INSTANTLY_TIKTOK_REENGAGEMENT_CAMPAIGN_ID;
  return null;
}

// ─── AI Reply Suggestion ─────────────────────────────────────────────────────

async function suggestReply(website, stage, lastReplyContent, dynamicFieldValue, pipelineType) {
  const pipelineContext = pipelineType === 'wholesale'
    ? 'We reached out to e-commerce companies offering a curated list of potential wholesale partners. When they expressed interest, we sent them the list and asked some qualifying questions.'
    : 'We reached out to e-commerce companies offering a curated list of top TikTok Shop affiliates in their niche. When they expressed interest, we sent them the list and asked some qualifying questions.';

  const prompt = `You are a helpful sales assistant. Here is the context for this outreach:

${pipelineContext}

Lead website: ${website || '(unknown)'}
Current stage: ${stage}
Their last message: "${lastReplyContent || '(no message content)'}"
${dynamicFieldValue ? `\nList content we sent them: "${dynamicFieldValue}"` : ''}

Write a concise follow-up reply (3–4 sentences max) that continues the conversation naturally. Be professional and friendly.
Output ONLY the reply text — no subject line, no preamble.`;

  try {
    const response = await axios.post(
      'https://openrouter.ai/api/v1/chat/completions',
      {
        model: 'google/gemini-2.0-flash-001',
        messages: [{ role: 'user', content: prompt }],
        max_tokens: 300,
      },
      {
        headers: {
          'Authorization': `Bearer ${process.env.OPENROUTER_API_KEY}`,
          'Content-Type': 'application/json',
          'HTTP-Referer': 'https://listcrm-production.up.railway.app',
          'X-Title': 'ListCRM',
        },
      }
    );
    return response.data.choices[0].message.content.trim();
  } catch (err) {
    console.error('[openrouter] suggestion failed:', err.response?.data || err.message);
    return null;
  }
}

// ─── Slack Helpers ────────────────────────────────────────────────────────────

function withSentStatus(blocks, statusText) {
  const filtered = (blocks || []).filter(b => !['ai_reply_actions', 'stage_actions'].includes(b.block_id));
  filtered.push({ type: 'context', elements: [{ type: 'mrkdwn', text: statusText }] });
  return filtered;
}

async function notifySlack(lead, oldStage, newStage, suggestedReply) {
  if (!process.env.SLACK_WEBHOOK_URL) return;

  let headerText;
  if (!oldStage) {
    headerText = `New Lead → ${newStage}`;
  } else if (oldStage === newStage) {
    headerText = `Reply Received — ${newStage}`;
  } else {
    headerText = `Stage Change: ${oldStage} → ${newStage}`;
  }

  const blocks = [
    {
      type: 'header',
      text: { type: 'plain_text', text: headerText },
    },
    {
      type: 'section',
      fields: [
        { type: 'mrkdwn', text: `*Website:*\n${lead.website || '—'}` },
        { type: 'mrkdwn', text: `*Email:*\n${lead.email}` },
        { type: 'mrkdwn', text: `*Campaign:*\n${lead.campaign || '—'}` },
        { type: 'mrkdwn', text: `*Stage:*\n${newStage}` },
      ],
    },
  ];

  // Show whichever dynamic field is populated
  const dynamicLabel  = lead.wholesaleField ? 'Wholesale Partner Ideas' : 'Top TikTok Shop Seller';
  const dynamicValue  = lead.wholesaleField || lead.tiktokField;
  if (dynamicValue) {
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: `*${dynamicLabel}:*\n${dynamicValue.substring(0, 2900)}` },
    });
  }

  if (lead.lastReply) {
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: `*Their last message:*\n${lead.lastReply.substring(0, 2900)}` },
    });
  }

  if (suggestedReply) {
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: `*Suggested reply (AI):*\n${suggestedReply}` },
    });
    blocks.push({
      type: 'actions',
      block_id: 'ai_reply_actions',
      elements: [
        {
          type: 'button',
          action_id: 'send_ai_reply',
          text: { type: 'plain_text', text: '✉️ Send Now', emoji: true },
          style: 'primary',
          value: lead.email,
          confirm: {
            title: { type: 'plain_text', text: 'Send this reply?' },
            text: { type: 'mrkdwn', text: `Send the AI-suggested reply to *${lead.email}* via Instantly?` },
            confirm: { type: 'plain_text', text: 'Send' },
            deny:    { type: 'plain_text', text: 'Cancel' },
          },
        },
        {
          type: 'button',
          action_id: 'edit_ai_reply',
          text: { type: 'plain_text', text: '✏️ Edit & Send', emoji: true },
          value: lead.email,
        },
      ],
    });
  }

  // Advance Stage button — only when not at final stage
  const stageIdx = STAGES.indexOf(newStage);
  if (stageIdx >= 0 && stageIdx < STAGES.length - 1) {
    const nextStage = STAGES[stageIdx + 1];
    blocks.push({
      type: 'actions',
      block_id: 'stage_actions',
      elements: [
        {
          type: 'button',
          action_id: 'advance_stage',
          text: { type: 'plain_text', text: `⬆️ Advance to ${nextStage}`, emoji: true },
          value: lead.email,
          confirm: {
            title:   { type: 'plain_text', text: 'Advance Stage?' },
            text:    { type: 'mrkdwn', text: `Move *${lead.website || lead.email}* from *${newStage}* → *${nextStage}*?` },
            confirm: { type: 'plain_text', text: 'Advance' },
            deny:    { type: 'plain_text', text: 'Cancel' },
          },
        },
      ],
    });
  }

  if (lead.uniboxUrl) {
    blocks.push({
      type: 'context',
      elements: [{ type: 'mrkdwn', text: `<${lead.uniboxUrl}|View conversation in Instantly Unibox →>` }],
    });
  }

  blocks.push({ type: 'divider' });

  try {
    await axios.post(process.env.SLACK_WEBHOOK_URL, { blocks });
  } catch (err) {
    console.error('[slack] notification failed:', err.message);
  }
}

// ─── Core: Advance Stage ─────────────────────────────────────────────────────

async function advanceStage(email, newStage, extraUpdates = {}) {
  const found       = await findRowByEmail(email);
  const currentStage = found?.rowData[COL.STAGE] || null;

  // Guard: don't move backward
  const currentIdx = STAGES.indexOf(currentStage);
  const newIdx     = STAGES.indexOf(newStage);
  if (currentIdx >= 0 && newIdx <= currentIdx) {
    console.log(`[advance] skipped — ${email} already at "${currentStage}", not moving to "${newStage}"`);
    return;
  }

  const today = new Date().toISOString().slice(0, 10);

  if (found) {
    const updates = {
      [COL.STAGE]:      newStage,
      [COL.STAGE_DATE]: today,
      ...extraUpdates,
    };
    await updateRow(found.rowIndex, updates);
  } else {
    // New lead — build a full row
    const row = new Array(14).fill('');
    row[COL.EMAIL]      = email;
    row[COL.STAGE]      = newStage;
    row[COL.STAGE_DATE] = today;
    Object.entries(extraUpdates).forEach(([col, val]) => { row[Number(col)] = val; });
    await appendRow(row);
  }

  // Fetch updated row for Slack notification
  const updatedFound = await findRowByEmail(email);
  const r = updatedFound?.rowData || [];

  const lead = {
    email,
    website:       r[COL.WEBSITE]         || '',
    campaign:      r[COL.CAMPAIGN]        || '',
    lastReply:     r[COL.LAST_REPLY]      || '',
    uniboxUrl:     r[COL.UNIBOX_LINK]     || '',
    wholesaleField: r[COL.WHOLESALE_FIELD] || '',
    tiktokField:   r[COL.TIKTOK_FIELD]    || '',
  };

  const pipelineType   = getPipelineType(r);
  const dynamicValue   = lead.wholesaleField || lead.tiktokField;
  const suggestion     = await suggestReply(lead.website, newStage, lead.lastReply, dynamicValue, pipelineType);
  if (suggestion && updatedFound) {
    await updateRow(updatedFound.rowIndex, { [COL.AI_REPLY]: suggestion });
  }

  await Promise.all([
    notifySlack(lead, currentStage, newStage, suggestion),
    updateInstantlyLabel(email, newStage),
  ]);

  console.log(`[advance] ${email}: "${currentStage || 'new'}" → "${newStage}"`);
}

// ─── Webhook: Instantly → ListCRM ────────────────────────────────────────────

app.post('/webhook/instantly', async (req, res) => {
  res.sendStatus(200); // Acknowledge immediately
  const body = req.body || {};

  // Log full payload on first few events to confirm field names
  console.log('[webhook] raw body:', JSON.stringify(body).substring(0, 600));

  try {
    const email = (body.lead_email || body.email || '').toLowerCase().trim();
    if (!email) return;

    const website  = body.website || body.company_domain || body.companyDomain || '';
    const campaign = body.campaign_name || body.campaign || '';
    const uniboxUrl = body.unibox_url
      || (body.lead_id ? `https://app.instantly.ai/app/unibox?lead_id=${body.lead_id}` : '');

    // Extract reply text (full, not truncated)
    const replyText    = body.reply_text || body.email_body || '';
    const replySnippet = replyText || body.reply_text_snippet || '';

    // Extract custom dynamic field variables
    // Instantly may nest these under custom_variables or send them at the top level
    const cv = body.custom_variables || body.variables || body;
    const wholesaleField = cv['Wholesale Partner Ideas'] || cv['wholesale_partner_ideas'] || '';
    const tiktokField    = cv['Top TikTok Shop Seller']  || cv['top_tiktok_shop_seller']  || '';

    const eventType = (body.event_type || body.type || '').toLowerCase();

    // ── Incoming: label / stage event ──────────────────────────────────────
    if (eventType === 'lead_interested' || eventType === 'interested') {
      await advanceStage(email, 'Interested', {
        ...(website      ? { [COL.WEBSITE]:         website }       : {}),
        ...(campaign     ? { [COL.CAMPAIGN]:        campaign }      : {}),
        ...(uniboxUrl    ? { [COL.UNIBOX_LINK]:     uniboxUrl }     : {}),
        ...(wholesaleField ? { [COL.WHOLESALE_FIELD]: wholesaleField } : {}),
        ...(tiktokField  ? { [COL.TIKTOK_FIELD]:    tiktokField }   : {}),
      });
      return;
    }

    // Direct stage name match (e.g. event_type === 'Provided List')
    if (STAGES.includes(body.event_type || body.type || '')) {
      const targetStage = body.event_type || body.type;
      await advanceStage(email, targetStage, {
        ...(website   ? { [COL.WEBSITE]:   website }  : {}),
        ...(campaign  ? { [COL.CAMPAIGN]:  campaign } : {}),
        ...(uniboxUrl ? { [COL.UNIBOX_LINK]: uniboxUrl } : {}),
      });
      return;
    }

    // Label ID / name match
    if (['lead_label_updated', 'custom_label', 'interest_label_updated'].includes(eventType)) {
      const labelMap = LABEL_MAP();
      const incomingLabelId   = body.label_id || body.labelId || '';
      const incomingLabelName = (body.label_name || body.labelName || '').trim();
      const matchedStage = Object.entries(labelMap).find(([, id]) => id && id === incomingLabelId)?.[0]
        || Object.keys(labelMap).find(s => s === incomingLabelName);
      if (matchedStage) {
        await advanceStage(email, matchedStage, {
          ...(uniboxUrl ? { [COL.UNIBOX_LINK]: uniboxUrl } : {}),
        });
      }
      return;
    }

    // ── email_sent: we sent the list → advance from Lead List Ready → Provided List
    if (eventType === 'email_sent') {
      const found = await findRowByEmail(email);
      const currentStage = found?.rowData[COL.STAGE];
      if (currentStage === 'Lead List Ready') {
        await advanceStage(email, 'Provided List', {
          ...(uniboxUrl ? { [COL.UNIBOX_LINK]: uniboxUrl } : {}),
        });
      }
      return;
    }

    // ── reply_received: save reply, generate AI suggestion, notify Slack ──
    // Stage is NOT auto-advanced — manual via Slack button or Sheet checkbox
    if (eventType === 'reply_received') {
      const found = await findRowByEmail(email);
      const currentStage = found?.rowData[COL.STAGE];

      if (!currentStage) {
        console.log(`[webhook] reply_received for unknown lead ${email} — no action`);
        return;
      }

      const r = found.rowData;
      const lead = {
        email,
        website:        r[COL.WEBSITE]         || website || '',
        campaign:       r[COL.CAMPAIGN]        || campaign || '',
        lastReply:      replySnippet,
        uniboxUrl:      uniboxUrl || r[COL.UNIBOX_LINK] || '',
        wholesaleField: r[COL.WHOLESALE_FIELD] || wholesaleField || '',
        tiktokField:    r[COL.TIKTOK_FIELD]    || tiktokField || '',
      };

      const updates = { [COL.LAST_REPLY]: replySnippet };
      if (uniboxUrl) updates[COL.UNIBOX_LINK] = uniboxUrl;
      await updateRow(found.rowIndex, updates);

      const pipelineType = getPipelineType(r);
      const dynamicValue = lead.wholesaleField || lead.tiktokField;
      const suggestion   = await suggestReply(lead.website, currentStage, replySnippet, dynamicValue, pipelineType);
      if (suggestion) {
        await updateRow(found.rowIndex, { [COL.AI_REPLY]: suggestion });
      }

      await notifySlack(lead, currentStage, currentStage, suggestion);
      console.log(`[reply] ${email} at "${currentStage}" replied — Slack notified, stage unchanged`);
    }

  } catch (err) {
    console.error('[webhook error]', err.message, err.stack);
  }
});

// ─── Cron: Daily Re-engagement Check ─────────────────────────────────────────
// Leads in "Provided List" for ≥ 3 days with no re-engagement yet → add to
// the appropriate re-engagement campaign and stamp col K.

const REENGAGEMENT_DAYS = 3;

cron.schedule('0 9 * * *', async () => {
  console.log('[cron] Running daily re-engagement check...');
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A:K`,
    });
    const rows  = res.data.values || [];
    const today = new Date();

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row) continue;

      const email      = row[COL.EMAIL];
      const stage      = row[COL.STAGE];
      const stageDate  = row[COL.STAGE_DATE];
      const reengaged  = row[COL.REENGAGED];

      if (!email || stage !== 'Provided List' || !stageDate) continue;
      if (reengaged) continue; // already re-engaged

      const days = (today - new Date(stageDate)) / 86400000;
      if (days < REENGAGEMENT_DAYS) continue;

      const pipelineType  = getPipelineType(row);
      const campaignId    = getReengagementCampaignId(pipelineType);
      const website       = row[COL.WEBSITE] || '';

      console.log(`[cron] re-engage: ${email} (${pipelineType}, ${days.toFixed(1)} days in "Provided List")`);

      await addToReengagementCampaign(email, website, campaignId);

      // Stamp col K with today's date to prevent double-adding
      const rowNum = i + 1;
      await sheets.spreadsheets.values.update({
        spreadsheetId: SHEET_ID,
        range: `${SHEET_NAME}!K${rowNum}`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [[today.toISOString().slice(0, 10)]] },
      });

      await new Promise(r => setTimeout(r, 500)); // rate-limit
    }

    console.log('[cron] Done.');
  } catch (err) {
    console.error('[cron error]', err.message, err.stack);
  }
});

// ─── Slack Interactions ───────────────────────────────────────────────────────

app.post('/slack/interactions', async (req, res) => {
  // ── Verify Slack signature ───────────────────────────────────────────────
  const signingSecret = process.env.SLACK_SIGNING_SECRET;
  const timestamp     = req.headers['x-slack-request-timestamp'];
  const slackSig      = req.headers['x-slack-signature'];

  if (signingSecret && timestamp && slackSig) {
    if (Math.abs(Date.now() / 1000 - Number(timestamp)) > 300) {
      return res.status(403).send('Request too old');
    }
    const baseStr = `v0:${timestamp}:${req.rawBody || ''}`;
    const hmac    = crypto.createHmac('sha256', signingSecret).update(baseStr).digest('hex');
    const expected = `v0=${hmac}`;
    if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(slackSig))) {
      return res.status(403).send('Invalid signature');
    }
  }

  let payload;
  try {
    payload = JSON.parse(req.body.payload);
  } catch {
    return res.status(400).send('Bad payload');
  }

  // ── block_actions: button clicks ─────────────────────────────────────────
  if (payload.type === 'block_actions') {
    res.status(200).send(); // ack immediately

    const action      = payload.actions[0];
    const email       = action.value;
    const responseUrl = payload.response_url;
    const origBlocks  = payload.message?.blocks;

    if (action.action_id === 'send_ai_reply') {
      try {
        const rows  = await getAllRows();
        const match = rows.find(r => (r[COL.EMAIL] || '').toLowerCase() === email.toLowerCase());
        const aiReply = match ? match[COL.AI_REPLY] : null;
        if (!aiReply) {
          await axios.post(responseUrl, { text: '⚠️ No AI reply found in sheet for this lead.', replace_original: false });
          return;
        }
        await sendInstantlyReply(email, aiReply);
        console.log(`[slack→instantly] sent AI reply for ${email}`);
        await axios.post(responseUrl, {
          replace_original: true,
          blocks: withSentStatus(origBlocks, `✅ *Reply sent via Instantly* — ${new Date().toLocaleString()}`),
        });
      } catch (err) {
        console.error('[slack] send_ai_reply error:', err.message);
        await axios.post(responseUrl, { text: `❌ Send failed: ${err.message}`, replace_original: false });
      }

    } else if (action.action_id === 'edit_ai_reply') {
      try {
        const rows    = await getAllRows();
        const match   = rows.find(r => (r[COL.EMAIL] || '').toLowerCase() === email.toLowerCase());
        const aiReply = match ? match[COL.AI_REPLY] : '';
        const modalRes = await axios.post('https://slack.com/api/views.open', {
          trigger_id: payload.trigger_id,
          view: {
            type:      'modal',
            callback_id: 'edit_ai_reply_modal',
            private_metadata: JSON.stringify({ email, responseUrl }),
            title:  { type: 'plain_text', text: 'Edit & Send Reply' },
            submit: { type: 'plain_text', text: '✉️ Send' },
            close:  { type: 'plain_text', text: 'Cancel' },
            blocks: [{
              type:       'input',
              block_id:   'reply_block',
              label:      { type: 'plain_text', text: 'Reply message' },
              element: {
                type:          'plain_text_input',
                action_id:     'reply_text',
                multiline:     true,
                initial_value: aiReply || '',
              },
            }],
          },
        }, { headers: { Authorization: `Bearer ${process.env.SLACK_BOT_TOKEN}` } });
        if (!modalRes.data?.ok) {
          console.error('[slack] views.open failed:', modalRes.data);
        }
      } catch (err) {
        console.error('[slack] edit_ai_reply error:', err.message);
      }

    } else if (action.action_id === 'advance_stage') {
      try {
        const rows  = await getAllRows();
        const match = rows.find(r => (r[COL.EMAIL] || '').toLowerCase() === email.toLowerCase());
        const currentStage = match ? match[COL.STAGE] : null;
        const currentIdx   = STAGES.indexOf(currentStage);
        if (currentIdx < 0) {
          await axios.post(responseUrl, { text: `⚠️ Lead not found or stage unknown.`, replace_original: false });
          return;
        }
        if (currentIdx >= STAGES.length - 1) {
          await axios.post(responseUrl, { text: `⚠️ Already at final stage: *${currentStage}*`, replace_original: false });
          return;
        }
        const nextStage = STAGES[currentIdx + 1];
        await advanceStage(email, nextStage);
        console.log(`[slack→advance] ${email}: ${currentStage} → ${nextStage}`);
        await axios.post(responseUrl, {
          replace_original: true,
          blocks: withSentStatus(origBlocks, `✅ *Stage advanced: ${currentStage} → ${nextStage}* — ${new Date().toLocaleString()}`),
        });
      } catch (err) {
        console.error('[slack] advance_stage error:', err.message);
        await axios.post(responseUrl, { text: `❌ Advance failed: ${err.message}`, replace_original: false });
      }
    }

  // ── view_submission: Edit modal → Send ──────────────────────────────────
  } else if (payload.type === 'view_submission' && payload.view?.callback_id === 'edit_ai_reply_modal') {
    res.status(200).json({});

    try {
      const { email, responseUrl } = JSON.parse(payload.view.private_metadata);
      const editedText = payload.view.state.values?.reply_block?.reply_text?.value || '';
      await sendInstantlyReply(email, editedText);
      console.log(`[slack→instantly] sent edited reply for ${email}`);
      await axios.post(responseUrl, {
        replace_original: true,
        text: `✅ *Edited reply sent via Instantly* to ${email} — ${new Date().toLocaleString()}`,
      });
    } catch (err) {
      console.error('[slack] view_submission send error:', err.message);
    }

  } else {
    res.status(200).send();
  }
});

// ─── Scraper Helpers ──────────────────────────────────────────────────────────

function parseScraperMessage(text) {
  const domainMatch = text.match(/\*\*Company domain:\*\*\s*([^\s\n•*]+)/i);
  const targetMatch = text.match(/\*\*Target list:\*\*\s*([^\n•*]+)/i);
  const driveMatch  = text.match(/Google Drive:\s*(https:\/\/drive\.google\.com\/\S+)/i);
  return {
    domain:     domainMatch?.[1]?.trim(),
    targetList: targetMatch?.[1]?.trim(),
    driveUrl:   driveMatch?.[1]?.trim(),
  };
}

function normalizeDomain(url) {
  if (!url) return '';
  return url
    .toLowerCase()
    .replace(/^https?:\/\//i, '')
    .replace(/^www\./i, '')
    .replace(/\/.*$/, '')   // strip path
    .trim();
}

// ─── Slack: Scraper Events ────────────────────────────────────────────────────
// Receives Events API callbacks from the scraper output channel.
// Parses the completed-run message, matches domain to a sheet row, and writes
// the Google Drive CSV link to col M or N based on which partner idea was used.

app.post('/slack/scraper-events', async (req, res) => {
  let body;
  try {
    body = JSON.parse(req.rawBody || '{}');
  } catch {
    return res.status(400).send('Bad JSON');
  }

  // URL verification challenge — must respond synchronously before sig check
  if (body.type === 'url_verification') {
    return res.json({ challenge: body.challenge });
  }

  // ── Verify Slack signature ───────────────────────────────────────────────
  const signingSecret = process.env.SLACK_SIGNING_SECRET;
  const timestamp     = req.headers['x-slack-request-timestamp'];
  const slackSig      = req.headers['x-slack-signature'];

  if (signingSecret && timestamp && slackSig) {
    if (Math.abs(Date.now() / 1000 - Number(timestamp)) > 300) {
      return res.status(403).send('Request too old');
    }
    const baseStr  = `v0:${timestamp}:${req.rawBody || ''}`;
    const hmac     = crypto.createHmac('sha256', signingSecret).update(baseStr).digest('hex');
    const expected = `v0=${hmac}`;
    if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(slackSig))) {
      return res.status(403).send('Invalid signature');
    }
  }

  res.sendStatus(200); // Ack immediately — process async

  (async () => {
    try {
      const event = body.event || {};

      // Only act on plain messages in the scraper channel; ignore bot posts & edits
      if (
        event.type    !== 'message' ||
        event.channel !== process.env.SLACK_SCRAPER_CHANNEL_ID ||
        event.bot_id  ||
        event.subtype
      ) return;

      const { domain, targetList, driveUrl } = parseScraperMessage(event.text || '');

      if (!domain || !driveUrl) {
        console.log('[scraper] skipping message — could not parse domain or Google Drive URL');
        return;
      }

      console.log(`[scraper] domain="${domain}" target="${targetList}" url=${driveUrl}`);

      // ── Find matching row by website domain ─────────────────────────────
      const rows      = await getAllRows();
      const normInput = normalizeDomain(domain);
      let idx         = rows.findIndex(r => normalizeDomain(r[COL.WEBSITE]) === normInput);

      let rowIndex;
      let currentStage   = '';
      let wholesaleField = '';

      if (idx === -1) {
        // Domain not in sheet — create a stub row so the CSV link is recorded
        console.warn(`[scraper] domain "${domain}" not found in sheet — creating stub row`);
        const stubRow = new Array(14).fill('');
        stubRow[COL.WEBSITE] = domain;
        await appendRow(stubRow);
        const freshRows = await getAllRows();
        idx = freshRows.findIndex(r => normalizeDomain(r[COL.WEBSITE]) === normInput);
        if (idx === -1) {
          console.error('[scraper] failed to locate newly appended row');
          return;
        }
        rowIndex = idx + 2;
      } else {
        rowIndex       = idx + 2;
        currentStage   = rows[idx][COL.STAGE]           || '';
        wholesaleField = rows[idx][COL.WHOLESALE_FIELD]  || '';
      }

      // ── Determine which partner column (M vs N) ──────────────────────────
      // WHOLESALE_FIELD format: "boutique retailers and museum stores"
      // Split on " and " (case-insensitive) to get partner ideas at index 0 and 1.
      const partners   = wholesaleField.split(/\s+and\s+/i).map(p => p.trim().toLowerCase()).filter(Boolean);
      const normTarget = (targetList || '').trim().toLowerCase();
      const partnerIdx = partners.indexOf(normTarget);

      const csvCol = partnerIdx === 1 ? COL.SCRAPED_CSV_2 : COL.SCRAPED_CSV_1;
      if (partnerIdx === -1 && targetList) {
        console.warn(`[scraper] target "${targetList}" not matched in "${wholesaleField}" — defaulting to Partner 1 column`);
      }

      const updates = { [csvCol]: driveUrl };

      // ── Auto-advance stage: Interested → Lead List Ready ─────────────────
      if (currentStage === 'Interested') {
        const today            = new Date().toISOString().slice(0, 10);
        updates[COL.STAGE]      = 'Lead List Ready';
        updates[COL.STAGE_DATE] = today;
        console.log(`[scraper] auto-advancing "${domain}": Interested → Lead List Ready`);
      }

      await updateRow(rowIndex, updates);
      console.log(`[scraper] updated row ${rowIndex} for "${domain}" — col ${partnerIdx === 1 ? 'N' : 'M'} = ${driveUrl}`);

    } catch (err) {
      console.error('[scraper error]', err.message, err.stack);
    }
  })();
});

// ─── Health Check ─────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ status: 'ok', timestamp: new Date() }));

// ─── Manual Stage Advance ─────────────────────────────────────────────────────
// Called by the Google Sheets "Advance Stage" checkbox via Apps Script.
// POST /api/advance  { email, stage? }  Authorization: Bearer <ADVANCE_TOKEN>

app.post('/api/advance', async (req, res) => {
  const token = (req.headers.authorization || '').replace('Bearer ', '');
  if (!process.env.ADVANCE_TOKEN || token !== process.env.ADVANCE_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { email, stage } = req.body || {};
  if (!email) return res.status(400).json({ error: 'email required' });

  try {
    const rows       = await getAllRows();
    const match      = rows.find(r => (r[COL.EMAIL] || '').toLowerCase() === email.toLowerCase());
    const currentStage = match ? match[COL.STAGE] : null;
    const currentIdx   = STAGES.indexOf(currentStage);

    let targetStage = stage || null;
    if (!targetStage) {
      if (currentIdx < 0) {
        return res.status(404).json({ error: 'Lead not found in sheet', email });
      }
      if (currentIdx >= STAGES.length - 1) {
        return res.status(400).json({ error: 'Already at final stage', currentStage });
      }
      targetStage = STAGES[currentIdx + 1];
    }

    await advanceStage(email, targetStage);
    console.log(`[advance] manual: ${email} → ${targetStage}`);
    res.json({ success: true, email, previousStage: currentStage, newStage: targetStage });
  } catch (err) {
    console.error('[advance] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Startup: Auto-join Slack channels ───────────────────────────────────────

async function joinSlackChannel(channelId, label) {
  const token = process.env.SLACK_BOT_TOKEN;
  if (!channelId || !token) return;
  try {
    const res = await axios.post(
      'https://slack.com/api/conversations.join',
      { channel: channelId },
      { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } }
    );
    if (res.data.ok) {
      console.log(`[startup] Joined ${label} channel ${channelId}`);
    } else {
      // already_in_channel is fine — anything else is worth logging
      if (res.data.error !== 'already_in_channel') {
        console.warn(`[startup] conversations.join (${label}):`, res.data.error);
      } else {
        console.log(`[startup] Already in ${label} channel ${channelId}`);
      }
    }
  } catch (err) {
    console.error(`[startup] Failed to join ${label} channel:`, err.message);
  }
}

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`ListCRM running on port ${PORT}`);
  // Auto-join both channels so the bot can post and receive events
  await joinSlackChannel(process.env.SLACK_SCRAPER_CHANNEL_ID,    'scraper');
  await joinSlackChannel(process.env.SLACK_NOTIFICATIONS_CHANNEL_ID, 'notifications');
});

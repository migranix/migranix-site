const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch'); // Ensure node-fetch or global fetch is active
const app = express();

app.use(cors());
app.use(express.json());

// Load Token securely from Render Environment Variables
const HF_TOKEN = process.env.HF_TOKEN;
const HF_ROUTER_URL = "https://router.huggingface.co/v1/chat/completions";
const CORE_MODEL = "Qwen/Qwen2.5-Coder-32B-Instruct";

/* ── Free Alternative Text-to-SQL Routing Engine ── */
app.post('/api/ai-sql', async (req, res) => {
  const { prompt, schema_context } = req.body;

  try {
    const response = await fetch(HF_ROUTER_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${HF_TOKEN}`
      },
      body: JSON.stringify({
        model: CORE_MODEL,
        messages: [
          { 
            role: "system", 
            content: "You are a professional compiler. Convert plain English directly into clean standard SQL syntax. Return ONLY raw executable query strings. Never wrap inside markdown symbols, do not include explanations, and omit commentary text completely." 
          },
          { 
            role: "user", 
            content: `Schema Context:\n${schema_context || "No active data catalogs schema connected."}\n\nTask: Convert this request to pure SQL: ${prompt}` 
          }
        ],
        max_tokens: 500,
        temperature: 0.1
      })
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error?.message || `Hugging Face gateway returned status ${response.status}`);
    }

    const data = await response.json();
    let cleanSql = data.choices[0].message.content.trim();
    
    // Safety guardrail strip block out markdown wraps if returned anyway
    cleanSql = cleanSql.replace(/^```sql\s*/i, '').replace(/^```\s*/, '').replace(/```$/, '').trim();

    return res.json({ success: true, sql: cleanSql });
  } catch (err) {
    console.error("Free SQL Engine Fail:", err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/* ── Free Alternative AI Explanation Controller ── */
app.post('/api/ai-explain', async (req, res) => {
  const { sql } = req.body;
  try {
    const response = await fetch(HF_ROUTER_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${HF_TOKEN}`
      },
      body: JSON.stringify({
        model: CORE_MODEL,
        messages: [
          { role: "system", content: "You are an expert data analyst. Explain what database operations the following SQL query performs in clear, plain English paragraphs. Keep it professional and structured." },
          { role: "user", content: `Explain this query:\n${sql}` }
        ]
      })
    });
    const data = await response.json();
    return res.json({ success: true, explanation: data.choices[0].message.content.trim() });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

/* ── Free Alternative AI Query Optimizer ── */
app.post('/api/ai-optimize', async (req, res) => {
  const { sql } = req.body;
  try {
    const response = await fetch(HF_ROUTER_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${HF_TOKEN}`
      },
      body: JSON.stringify({
        model: CORE_MODEL,
        messages: [
          { role: "system", content: "Analyze the provided SQL query statement for computational inefficiencies, index misses, or structural flaws. List exact bullet points indicating optimizations or provide a refactored SQL alternative." },
          { role: "user", content: `Optimize this statement:\n${sql}` }
        ]
      })
    });
    const data = await response.json();
    return res.json({ success: true, optimization: data.choices[0].message.content.trim() });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
});

// Start server block rules...
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Ecosystem Server active on port ${PORT}`));

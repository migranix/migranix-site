const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch'); // Ensure node-fetch or global fetch is active
const app = express();

app.use(cors());
app.use(express.json());

// Securely access configuration tokens mapped from Render console parameters
const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_ROUTER_URL = "https://api.groq.com/openai/v1/chat/completions";
const CORE_MODEL = "llama-3.1-70b-versatile"; 

/* ── Groq API Text-to-SQL Performance Compiler Engine ── */
app.post('/api/ai-sql', async (req, res) => {
  const { prompt, schema_context } = req.body;

  try {
    const response = await fetch(GROQ_ROUTER_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${GROQ_API_KEY}`
      },
      body: JSON.stringify({
        model: CORE_MODEL,
        messages: [
          { 
            role: "system", 
            content: "You are a professional compiler. Convert plain English directly into clean, valid, executable standard SQL syntax. Return ONLY raw executable query strings. Never wrap inside markdown symbols, do not include explanations, and omit commentary text completely." 
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
      throw new Error(errorData.error?.message || `Groq Gateway communication error status ${response.status}`);
    }

    const data = await response.json();
    let cleanSql = data.choices[0].message.content.trim();
    
    // Safety guardrail strip block out markdown wraps if returned anyway
    cleanSql = cleanSql.replace(/^```sql\s*/i, '').replace(/^```\s*/, '').replace(/```$/, '').trim();

    return res.json({ success: true, sql: cleanSql });
  } catch (err) {
    console.error("Groq Engine compilation fault:", err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// Setup fallback pipeline tracking options...
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Ecosystem Server active on port ${PORT}`));

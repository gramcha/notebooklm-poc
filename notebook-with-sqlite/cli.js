#!/usr/bin/env node

// excel-to-sqlite.js
// CLI: Convert Excel to SQLite DB with inferred types and query with Ollama LLM
const xlsx = require("xlsx");
const Database = require("better-sqlite3");
const fs = require("fs");
const axios = require("axios");
const readline = require("readline");
const { Command } = require("commander");
const program = new Command();

function detectType(value) {
  console.log({value, is_dateTime: value instanceof Date});

  if (value === null || value === undefined || (typeof value === 'string' && value.trim() === '')) {
    return 'NULL';
  }
  if(value instanceof Date) {
    return 'JS_DATE';
  }
  if (typeof value === 'boolean') {
    return 'BOOLEAN';
  }

  // Detect Excel serial date (number in range)
  // if (typeof value === 'number' && value > 30000 && value < 60000) {
  //   return 'EXCEL_DATE';
  // }

  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'INTEGER' : 'REAL';
  }

  if (typeof value === 'string') {
    const val = value.trim();

    // ISO date
    if (/^\d{4}-\d{2}-\d{2}$/.test(val)) return 'DATE';
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(val)) return 'DATETIME';

    // DD/MM/YYYY or MM/DD/YYYY
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(val)) return 'DATE';

    // DD/MM/YYYY HH:MM:SS AM/PM
    if (/^\d{2}\/\d{2}\/\d{4}\s+\d{2}:\d{2}:\d{2}\s+(AM|PM)$/i.test(val)) return 'DATETIME';

    // HH:MM:SS or HH:MM
    if (/^\d{2}:\d{2}(:\d{2})?$/.test(val)) return 'TIME';

    // Numeric string
    if (!isNaN(Number(val))) {
      return Number.isInteger(Number(val)) ? 'INTEGER' : 'REAL';
    }

    // Boolean as string
    if (['true', 'false', 'yes', 'no'].includes(val.toLowerCase())) return 'BOOLEAN';
  }

  return 'TEXT';
}


console.log(detectType("12/12/2023 12:12:12 AM"));
function excelSerialDateToJSDate(serial) {
  if (typeof serial !== 'number' || isNaN(serial)) {
    throw new Error('Input must be a valid Excel serial number');
  }

  const utcDays = Math.floor(serial - 25569); // 25569 = days from 1900-01-01 to 1970-01-01
  const utcValue = utcDays * 86400; // seconds since epoch
  const fractionalDay = serial - Math.floor(serial) + 0.0000001;

  let totalSeconds = Math.floor(86400 * fractionalDay);
  const hours = Math.floor(totalSeconds / 3600);
  totalSeconds -= hours * 3600;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds - minutes * 60;

  const date = new Date(utcValue * 1000);
  date.setUTCHours(hours);
  date.setUTCMinutes(minutes);
  date.setUTCSeconds(seconds);

  return date;
}

function buildPromptWithHistory(userQuestion, contextMemory, tableName, schemaText) {
  // const schemaText = contextMemory.schema;
  const historyText = contextMemory.chatHistory
    .slice(-3)
    .map(
      (entry, i) => `Q${i + 1}: ${entry.question}\nA${i + 1}: ${entry.answer}`
    )
    .join("\n");

  const filterText = Object.entries(contextMemory.filters)
    .map(([k, v]) => `${tableName}.${k} = '${v}'`)
    .join(" AND ");

  const vague = isVagueQuestion(userQuestion);

  if (vague && !filterText) {
    return `The user asked a vague follow-up: "${userQuestion}". No prior filters are known.

Suggest asking for more specific information or provide an overview of key fields from this table:

Schema: ${schemaText}

Example: "You can ask about Delivered, Freight Cost, EDD, or Transporter Name."`;
  }

  return `
Given the schema:
${schemaText}

And recent conversation:
${historyText}

Known filters: ${filterText || "none"}

Generate an optimized SQLite query for the user question:
"${userQuestion}"

Only return the optimized SQL query.`.trim();
}
function buildBasicSQLPrompt(schema, userQuestion) {
  return `Given the schema: ${schema}, 
  write an SQLite query to answer: ${userQuestion}
  Do not return anything other than the query.`.trim();
}
function isVagueQuestion(question) {
  // const vagueKeywords = [
  //   "what",
  //   "which",
  //   "who",
  //   "when",
  //   "where",
  //   "why",
  //   "how",
  //
  //   "tell me",
  //   "give me",
  //   "what's",
  //   "who's",
  //   "when's",
  //   "where's",
  //   "why's",
  //   "how's",
  // ];
  //
  // return vagueKeywords.some((keyword) => question.toLowerCase().includes(keyword));
  return false;
}
function extractWhereConditions(sql) {
  const whereClauseMatch = sql.match(/where\s+(.+)/i);
  if (!whereClauseMatch) return {};

  const conditions = whereClauseMatch[1]
    .split(/\s+and\s+/i)
    .map(cond => cond.trim())
    .filter(Boolean);

  const filterMap = {};
  for (const cond of conditions) {
    const match = cond.match(/"?([\w\d_]+)"?\s*=\s*['"]?([^'"]+)['"]?/);
    if (match) {
      filterMap[match[1]] = match[2];
    }
  }
  return filterMap;
}


function excelToSQLite(excelFilePath, sqliteFilePath) {
  const workbook = xlsx.readFile(excelFilePath, { cellDates: true });
  const sheetName = workbook.SheetNames[0];
  const jsonData = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);

  if (jsonData.length === 0) throw new Error("No data in Excel sheet");

  const db = new Database(sqliteFilePath);
  const tableName = sheetName.replace(/\W/g, "_");
  const sampleRow = jsonData[0];

  const columnTypes = {};
  Object.keys(sampleRow).forEach(col => {
    const values = jsonData.map(row => row[col]).filter(v => v !== undefined && v !== null);
    console.log({col});
    let detected = detectType(values[0]);
    console.log({detected});
    if(detected === 'JS_DATE') {
      jsonData.forEach((row) => {
        if(row[col] !== undefined && row[col] !== null && row[col] instanceof Date) {
          row[col] = row[col].toISOString();
        }
      });
      detected = 'DATETIME';
    }
    columnTypes[col] = detected;
  });

  const columnDefs = Object.entries(columnTypes)
    .map(([key, type]) => `${key.replace(/\W/g, "_")} ${type}`)
    .join(", ");
  console.log({columnTypes});
  console.log({columnDefs});
  db.exec(`DROP TABLE IF EXISTS ${tableName};`);
  db.exec(`CREATE TABLE ${tableName} (${columnDefs});`);

  const columns = Object.keys(sampleRow);
  const sanitizedColumns = columns.map(k => k.replace(/\W/g, "_"));

  const insertStmt = db.prepare(
    `INSERT INTO ${tableName} (${sanitizedColumns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`
  );

  const insertMany = db.transaction((rows) => {
    for (const row of rows) {
      try {
        // Ensure consistent column order
        const values = columns.map(col => row[col]);
        insertStmt.run(values);
      } catch (err) {
        console.error(err.stack);
        throw err;
      }
    }
  });
  insertMany(jsonData);
  console.log(`✔️ SQLite DB with typed schema created at ${sqliteFilePath}`);
  return tableName;
}
function stripCodeFences(text) {
  return text.replace(/```(?:\w+)?\n?([\s\S]*?)```/g, '$1').trim();
}
const generateHumanReadableAnswer = async (userQuestion, sqlResult) => {
  const formatterPrompt = `
Based on the user query:
"${userQuestion}"

And the SQL result:
${JSON.stringify(sqlResult, null, 2)}

Generate a natural language answer. Do not talk about SQL.`;

  const response = await axios.post("http://localhost:11434/api/generate", {
    // model: "llama3",
    model: "mistral-small:latest",
    prompt: formatterPrompt,
    stream: false
  });

  let answer = response.data.response.trim();
  return answer;
};
let loadedDB = null;
const contextMemory = {
  schema: null, // generated once
  chatHistory: [], // { question, sql, result, answer }
  filters: {}, // extracted WHERE conditions
};
async function queryWithLLM(sqliteFilePath, tableName, userQuestion) {

  if(!loadedDB) {
    console.time("db load");
    loadedDB =  new Database(sqliteFilePath);
    console.timeEnd("db load");
  }
  const db = loadedDB;
  const columnsRes = db.prepare(`PRAGMA table_info(${tableName});`).all();
  // const schema = columnsRes.map(col => `${col.name} (${col.type})`).join(", ");
  const schema = columnsRes
    .map(col => `${tableName}.${col.name} (${col.type})`)
    .join(", ");
  // const prompt = `Given the schema: ${schema},
  // write an SQLite query to answer: ${userQuestion}
  // Do not return anything other than the query.
  // `;
  // const prompt = buildBasicSQLPrompt(schema, userQuestion);
  const prompt = buildPromptWithHistory( userQuestion, contextMemory, tableName, schema);
  console.time("prompt 1");
  // console.log("\n🤖 LLM Prompt:\n", prompt);
  const response = await axios.post("http://localhost:11434/api/generate", {
    // model: "llama3",
    model: "mistral-small:latest",
    prompt: prompt,
    stream: false
  });

  let sql = response.data.response.trim();
  if(sql) {
    sql = stripCodeFences(sql);
  }
  console.timeEnd("prompt 1");
  console.log("\nGenerated SQL:\n", sql);
  let humanReadableAnswer = "NA";
  try {
    console.time("query execution");
    const result = db.prepare(sql).all();
    console.log("\nQuery Result:\n", result);
    console.timeEnd("query execution");
    console.time("prompt 2");
    humanReadableAnswer = await generateHumanReadableAnswer(userQuestion, result);
    console.timeEnd("prompt 2");
    contextMemory.chatHistory.push({ question: userQuestion, sql, result, humanReadableAnswer });
    Object.assign(contextMemory.filters, extractWhereConditions(sql));
  } catch (err) {
    console.error("\n❌ Failed to execute generated SQL:\n", err.message);
  } finally {
    console.log("\n🤖 neerja: ", humanReadableAnswer);
  }
}

async function interactiveShell(sqliteFilePath, tableName) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  console.log(`💬 Interactive mode: Ask questions about '${tableName}' in '${sqliteFilePath}'. Type 'exit' to quit.`);

  while (true) {
    const userQuestion = await new Promise(resolve => rl.question("\n❓ Your question: ", resolve));
    if (userQuestion.toLowerCase() === 'exit') break;
    await queryWithLLM(sqliteFilePath, tableName, userQuestion);
  }

  rl.close();
}

program
  .command("convert")
  .description("Convert Excel file to SQLite DB")
  .requiredOption("-i, --input <file>", "Input Excel file path")
  .requiredOption("-o, --output <file>", "Output SQLite DB file path")
  .action((opts) => {
    excelToSQLite(opts.input, opts.output);
  });
//  node notebook-with-sqlite/cli.js convert -i "/Users/gramcha/sources/notebooklm-poc/input-excels/isr.xlsx" -o "/Users/gramcha/sources/notebooklm-poc/output-sqlite/isr.db"


program
  .command("ask")
  .description("Ask a question to the SQLite DB via Ollama LLM")
  .requiredOption("-d, --db <file>", "SQLite DB file path")
  .requiredOption("-t, --table <name>", "Table name to query")
  .requiredOption("-q, --question <text>", "Natural language question")
  .action((opts) => {
    queryWithLLM(opts.db, opts.table, opts.question);
  });

  // node notebook-with-sqlite/cli.js ask -d "/Users/gramcha/sources/notebooklm-poc/output-sqlite/isr.db" -t "Raw_Data" -q "tell about indent_id MAR-R144-M-21024 ?"

program
  .command("shell")
  .description("Interactive shell to ask multiple questions")
  .requiredOption("-d, --db <file>", "SQLite DB file path")
  .requiredOption("-t, --table <name>", "Table name to query")
  .action((opts) => {
    interactiveShell(opts.db, opts.table);
  });
// node notebook-with-sqlite/cli.js ask -d "/Users/gramcha/sources/notebooklm-poc/output-sqlite/isr.db" -t "Raw_Data"
program.parse(process.argv);

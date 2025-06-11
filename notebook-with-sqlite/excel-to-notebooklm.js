#!/usr/bin/env node

const xlsx = require("xlsx");
const Database = require("better-sqlite3");
const fs = require("fs");
const axios = require("axios");
const readline = require("readline");
const {Command} = require("commander");

class ExcelToNotebooklm {
  constructor() {
    this.loadedDB = null;
    this.contextMemory = {
      schema: null,
      chatHistory: [],
      filters: {},
    };
  }

  detectType(value) {
    if (value === null || value === undefined || (typeof value === 'string' && value.trim() === '')) {
      return 'NULL';
    }
    if (value instanceof Date) {
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

  buildPromptWithHistory(userQuestion, tableName, schemaText) {
    const historyText = this.contextMemory.chatHistory
      .slice(-3)
      .map((entry, i) => `Q${i + 1}: ${entry.question}\nA${i + 1}: ${entry.answer}`)
      .join("\n");

    const filterText = Object.entries(this.contextMemory.filters)
      .map(([k, v]) => `${tableName}.${k} = '${v}'`)
      .join(" AND ");

    const vague = this.isVagueQuestion(userQuestion);

    if (vague && !filterText) {
      return `The user asked a vague follow-up: "${userQuestion}". No prior filters are known.
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

Only return the optimized SQL query. Apply max limit of 30 rows`.trim();
  }

  isVagueQuestion(question) {
    return false; // Implement your vague question logic here
  }

  buildBasicSQLPrompt(schema, userQuestion) {
    return `Given the schema: ${schema}, 
  write an SQLite query to answer: ${userQuestion}
  Do not return anything other than the query.`.trim();
  }

  extractWhereConditions(sql) {
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

  excelToSQLite(excelFilePath, sqliteFilePath) {
    const workbook = xlsx.readFile(excelFilePath, {cellDates: true});
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
      let detected = this.detectType(values[0]);
      console.log({detected});
      if (detected === 'JS_DATE') {
        jsonData.forEach((row) => {
          if (row[col] !== undefined && row[col] !== null && row[col] instanceof Date) {
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

  stripCodeFences(text) {
    return text.replace(/```(?:\w+)?\n?([\s\S]*?)```/g, '$1').trim();
  }

  async generateHumanReadableAnswer(userQuestion, sqlResult) {
    const formatterPrompt = `
      Based on the user query:
      "${userQuestion}"
      And the SQL result:
      ${JSON.stringify(sqlResult, null, 2)}
      Generate a natural language answer. Do not talk about SQL.`;

    const response = await axios.post("http://localhost:11434/api/generate", {
      model: "mistral-small:latest",
      prompt: formatterPrompt,
      stream: false
    });

    return response.data.response.trim();
  }


  async queryWithLLM(sqliteFilePath, tableName, userQuestion) {
    if(!this.loadedDB) {
      console.time("db load");
      this.loadedDB =  new Database(sqliteFilePath);
      console.timeEnd("db load");
    }
    const db = this.loadedDB;
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
    const prompt = this.buildPromptWithHistory( userQuestion, this.contextMemory, tableName, schema);
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
      sql = this.stripCodeFences(sql);
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
      humanReadableAnswer = await this.generateHumanReadableAnswer(userQuestion, result);
      console.timeEnd("prompt 2");
      this.contextMemory.chatHistory.push({ question: userQuestion, sql, result, humanReadableAnswer });
      Object.assign(this.contextMemory.filters, this.extractWhereConditions(sql));
    } catch (err) {
      console.error("\n❌ Failed to execute generated SQL:\n", err.message);
    } finally {
      console.log("\n🤖 neerja: ", humanReadableAnswer);
    }
  }

  async interactiveShell(sqliteFilePath, tableName) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    console.log(`💬 Interactive mode: Ask questions about '${tableName}' in '${sqliteFilePath}'. Type 'exit' to quit.`);

    while (true) {
      const userQuestion = await new Promise(resolve => rl.question("\n❓ Your question: ", resolve));
      if (userQuestion.toLowerCase() === 'exit') break;
      await this.queryWithLLM(sqliteFilePath, tableName, userQuestion);
    }

    rl.close();
  }

  setupCLI() {
    const program = new Command();

    program
      .command("convert")
      .description("Convert Excel file to SQLite DB")
      .requiredOption("-i, --input <file>", "Input Excel file path")
      .requiredOption("-o, --output <file>", "Output SQLite DB file path")
      .action((opts) => {
        this.excelToSQLite(opts.input, opts.output);
      });
    //  node notebook-with-sqlite/excel-to-notebooklm.js convert -i "/Users/gramcha/sources/notebooklm-poc/input-excels/isr.xlsx" -o "/Users/gramcha/sources/notebooklm-poc/output-sqlite/isr.db"

    program
      .command("ask")
      .description("Ask a question to the SQLite DB via Ollama LLM")
      .requiredOption("-d, --db <file>", "SQLite DB file path")
      .requiredOption("-t, --table <name>", "Table name to query")
      .requiredOption("-q, --question <text>", "Natural language question")
      .action(async (opts) => {
        await this.queryWithLLM(opts.db, opts.table, opts.question);
      });
    // node notebook-with-sqlite/excel-to-notebooklm.js ask -d "/Users/gramcha/sources/notebooklm-poc/output-sqlite/isr.db" -t "Raw_Data" -q "tell about indent_id MAR-R144-M-21024 ?"

    program
      .command("shell")
      .description("Interactive shell to ask multiple questions")
      .requiredOption("-d, --db <file>", "SQLite DB file path")
      .requiredOption("-t, --table <name>", "Table name to query")
      .action(async (opts) => {
        await this.interactiveShell(opts.db, opts.table);
      });
    // node notebook-with-sqlite/excel-to-notebooklm.js shell -d "/Users/gramcha/sources/notebooklm-poc/output-sqlite/isr.db" -t "Raw_Data"

    program.parse(process.argv);
  }
}

// Usage
const converter = new ExcelToNotebooklm();
converter.setupCLI();

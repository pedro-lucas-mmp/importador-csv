import fs from "fs";
import csvParser from "csv-parser";
import { Pool } from "pg";
import { v4 as uuidv4 } from "uuid";
import 'dotenv/config';

// >> Configurações
const test = false // Set false para injeção DB
const copyAndPaste = true // Set true para copiar e colar no terminal

const merchant_id = "4d9a292e-de49-4fa4-8ed8-f3672246ff3c";

const fieldMap = {
  "Código (SKU)": "code",
  "Descrição": "description",
  "Preço": "price",
  "Observações": "details",
  "Situação": "listed",
  "Estoque": "quantity",
  "Categoria": "category",
  "Marca": "brand",
  "Formato embalagem": "package",
  "Unidade": "measuring_unit",
  "Origem": "origin",
  "Preço de custo": "cost",
  "Estoque mínimo": "minimal_quantity",
  "Peso líquido (Kg)": "weight",
  "GTIN/EAN": "gtin_ean",
  "CEST": "tax_categories"
};
// << Configurações
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

function mapCsvRowToDb(row, fieldMap) {
  const mapped = {};
  for (const [csvField, dbField] of Object.entries(fieldMap)) {
    if (row.hasOwnProperty(csvField)) {
      mapped[dbField] = row[csvField];
    }
  }
  return mapped;
}

function getCurrentTimestamp() {
  return new Date().toISOString();
}

async function insertFromCSV(filePath) {
  const client = (!copyAndPaste) ? await pool.connect() : null;
  try {
    if (!copyAndPaste) await client.query("BEGIN");

    const rows = [];
    fs.createReadStream(filePath)
      .pipe(csvParser({ separator: ";" }))
      .on("data", (row) => {
        //console.log(row);
        rows.push(row);
      })
      .on("end", async () => {
        console.log(`Lendo ${rows.length} registros do CSV...`);

        const mappedRows = rows.map(row => mapCsvRowToDb(row, fieldMap));

        const batchSize = 500;
        for (let i = 0; i < mappedRows.length; i += batchSize) {
          const chunk = mappedRows.slice(i, i + batchSize);

          const values = [];
          const params = [];
          let paramIndex = 1;

          chunk.forEach((r) => {
            const id = uuidv4();
            const timestamp = getCurrentTimestamp();
            values.push(
                `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7}, $${paramIndex + 8}, $${paramIndex + 9}, $${paramIndex + 10}, $${paramIndex + 11}, $${paramIndex + 12}, $${paramIndex + 13}, $${paramIndex + 14}, $${paramIndex + 15}, $${paramIndex + 16}, $${paramIndex + 17}, $${paramIndex + 18}, $${paramIndex + 19})`
            );
            paramIndex += 20;
            params.push(
                id,
                r.code,
                r.description,
                r.price ? parseFloat(r.price) : null,
                r.details || null,
                r.listed === "Ativo",
                r.quantity ? parseInt(r.quantity) : null,
                r.category || null,
                r.brand || null,
                r.package ? JSON.stringify(r.package) : null,
                r.measuring_unit || null,
                r.origin || null,
                r.cost ? parseFloat(r.cost) : null,
                r.minimal_quantity ? parseInt(r.minimal_quantity) : null,
                r.weight ? parseFloat(r.weight) : null,
                r.gtin_ean || null,
                r.tax_categories ? JSON.stringify(r.tax_categories) : null,
                merchant_id,
                timestamp,
                timestamp
            );
        });

        if (!test && copyAndPaste) {
            const sqlValues = [];
            for (let j = 0; j < chunk.length; j++) {
                const offset = j * 20;
                sqlValues.push(
                    `('${params[offset]}', '${params[offset + 1]}', '${params[offset + 2]}', ${params[offset + 3]}, ` +
                    `'${params[offset + 4]}', ${params[offset + 5]}, ${params[offset + 6]}, '${params[offset + 7]}', ` +
                    `'${params[offset + 8]}', '${params[offset + 9]}', '${params[offset + 10]}', '${params[offset + 11]}', ` +
                    `${params[offset + 12]}, ${params[offset + 13]}, ${params[offset + 14]}, '${params[offset + 15]}', ` +
                    `'${params[offset + 16]}', '${params[offset + 17]}', '${params[offset + 18]}', '${params[offset + 19]}')`
                );
            }
            const sql = `
                INSERT INTO products (
                id, code, description, price, details, listed, quantity, category, brand, package,
                measuring_unit, origin, cost, minimal_quantity, weight, gtin_ean, tax_categories, merchant_id, inserted_at, updated_at
                ) VALUES
                ${sqlValues.join(",\n")};
            `;

            fs.appendFileSync("insert_copia_e_cola.sql", sql + "\n");
            console.log("Arquivo 'insert_copia_e_cola.sql' gerado/atualizado com sucesso!");
            continue;
        }

        if (!test && !copyAndPaste) {
            await client.query(
            `INSERT INTO products (
                id, code, description, price, details, listed, quantity, category, brand, package,
                measuring_unit, origin, cost, minimal_quantity, weight, gtin_ean, tax_categories, merchant_id, inserted_at, updated_at
            ) VALUES ${values.join(",")}`,
            params
            );
        } else {
            console.log(`❗ Modo de teste ativo. Não foi feita nenhuma inserção no banco de dados para este lote de ${chunk.length} registros.\n`);
            console.log(`❓ Exibindo os primeiros 20 registros do lote: \n`);
            chunk.slice(0, 20).forEach((r, idx) => {
                console.log({
                    id: params[idx * 20],
                    code: r.code,
                    description: r.description,
                    price: r.price,
                    details: r.details,
                    listed: r.listed,
                    quantity: r.quantity,
                    category: r.category,
                    brand: r.brand,
                    package: r.package,
                    measuring_unit: r.measuring_unit,
                    origin: r.origin,
                    cost: r.cost,
                    minimal_quantity: r.minimal_quantity,
                    weight: r.weight,
                    gtin_ean: r.gtin_ean,
                    tax_categories: r.tax_categories,
                    merchant_id: merchant_id,
                    inserted_at: params[idx * 20 + 18],
                    updated_at: params[idx * 20 + 19]
                });
            });
        };
        }

        if (!copyAndPaste) {
          await client.query("COMMIT");
          client.release();
          console.log("✅ Importação concluída!");
        }
      });
  } catch (err) {
    if (!copyAndPaste) {
      await client.query("ROLLBACK");
      client.release();
    }
    console.error("Erro:", err);
  }
}

insertFromCSV("./csv-to-import/produtos_2025-08-11-17-25-16.csv");
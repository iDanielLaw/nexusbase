import { NexusBaseClient } from './client';
import { tsdb } from './proto/api/tsdb/tsdb';

async function main() {
  console.log('Connecting to NexusBase server...');
  const client = new NexusBaseClient({
    host: '10.1.1.1',
    port: 50051, // Default gRPC port
  });

  try {
    // --- 1. เขียนข้อมูล (Write a data point) ---
    const dataPoint = {
      metric: 'cpu.usage.test',
      tags: { host: 'client-ts-1', region: 'local' },
      fields: {
        value_percent: Math.random() * 100,
        core: 1,
        active: true,
      },
      ts: new Date(),
    };
    console.log('\nWriting data point:', JSON.stringify(dataPoint, null, 2));
    await client.put(dataPoint);
    console.log('✅ Successfully wrote data point.');

    // รอสักครู่เพื่อให้แน่ใจว่าข้อมูลถูก index แล้ว
    await new Promise(resolve => setTimeout(resolve, 100));

    // --- 2. ดึงข้อมูลดิบ (Query for the raw data point) ---
    const startTime = new Date(Date.now() - 5 * 60 * 1000); // 5 นาทีที่แล้ว
    const endTime = new Date();

    console.log(`\nQuerying raw data for '${dataPoint.metric}' from ${startTime.toISOString()} to ${endTime.toISOString()}`);

    const rawQueryStream = client.query({
      metric: dataPoint.metric,
      startTime,
      endTime,
      tags: { host: 'client-ts-1' },
    });

    let foundRaw = false;
    for await (const result of rawQueryStream) {
      console.log('  -> Found raw result:', JSON.stringify(result, null, 2));
      foundRaw = true;
    }
    if (!foundRaw) {
      console.log('  -> No raw results found.');
    }
    console.log('✅ Raw data query complete.');


    // --- 3. ดึงข้อมูลแบบสรุปรวม (Query with aggregation) ---
    console.log(`\nQuerying aggregated data for '${dataPoint.metric}' with a 1m interval.`);
    const aggQueryStream = client.query({
        metric: dataPoint.metric,
        startTime,
        endTime,
        tags: { host: 'client-ts-1' },
        aggregations: [
          { func: tsdb.AggregationSpec.AggregationFunc.AVERAGE, field: 'value_percent' },
          { func: tsdb.AggregationSpec.AggregationFunc.COUNT, field: 'value_percent' }
        ],
        downsampleInterval: '1m'
      });
  
      let foundAgg = false;
      for await (const result of aggQueryStream) {
        console.log('  -> Found aggregated result:', JSON.stringify(result, null, 2));
        foundAgg = true;
      }
      if (!foundAgg) {
        console.log('  -> No aggregated results found.');
      }
      console.log('✅ Aggregated data query complete.');

  } catch (error) {
    console.error('❌ An error occurred:', error);
  } finally {
    // --- 4. ปิดการเชื่อมต่อ (Close the connection) ---
    client.close();
    console.log('\nConnection closed.');
  }
}

main();
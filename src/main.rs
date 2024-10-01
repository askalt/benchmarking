use datafusion::{
    arrow::array::RecordBatch,
    execution::TaskContext,
    physical_plan::{collect, ExecutionPlan},
    prelude::SessionConfig,
};
use std::{
    env,
    io::{self, Write},
    sync::Arc,
    time::Instant,
};

use datafusion::{
    arrow::{self},
    prelude::{CsvReadOptions, SessionContext},
};

async fn collect_exec_plan(
    plan: Arc<dyn ExecutionPlan>,
    context: &SessionContext,
) -> datafusion::common::Result<Vec<RecordBatch>> {
    let task_ctx = Arc::new(TaskContext::from(&context.state()));
    collect(plan, task_ctx).await
}

#[tokio::main]
async fn main() {
    // Parse args.
    let args: Vec<String> = env::args().collect();

    let csv_path = if args.len() == 1 {
        "./data/example.csv"
    } else {
        &args[1]
    };

    // Create data source.
    let ctx = SessionContext::new();
    ctx.register_csv("bench", &csv_path, CsvReadOptions::new())
        .await
        .unwrap();

    let query = " SELECT MAX(attribute0), MAX(attribute1), MAX(attribute2), MAX(attribute3), MAX(attribute4), MAX(attribute5), MAX(attribute6), MAX(attribute7), MAX(attribute8), MAX(attribute9), MAX(attribute10), MAX(attribute11), MAX(attribute12), MAX(attribute13), MAX(attribute14), MAX(attribute15), MAX(attribute16), MAX(attribute17), MAX(attribute18), MAX(attribute19), MAX(attribute20), MAX(attribute21), MAX(attribute22), MAX(attribute23), MAX(attribute24), MAX(attribute25), MAX(attribute26), MAX(attribute27), MAX(attribute28), MAX(attribute29), MAX(attribute30), MAX(attribute31), MAX(attribute32), MAX(attribute33), MAX(attribute34), MAX(attribute35), MAX(attribute36), MAX(attribute37), MAX(attribute38), MAX(attribute39), MAX(attribute40), MAX(attribute41), MAX(attribute42), MAX(attribute43), MAX(attribute44), MAX(attribute45), MAX(attribute46), MAX(attribute47), MAX(attribute48), MAX(attribute49), MAX(attribute50), MAX(attribute51), MAX(attribute52), MAX(attribute53), MAX(attribute54), MAX(attribute55), MAX(attribute56), MAX(attribute57), MAX(attribute58), MAX(attribute59), MAX(attribute60), MAX(attribute61), MAX(attribute62), MAX(attribute63), MAX(attribute64), MAX(attribute65), MAX(attribute66), MAX(attribute67), MAX(attribute68), MAX(attribute69), MAX(attribute70), MAX(attribute71), MAX(attribute72), MAX(attribute73), MAX(attribute74), MAX(attribute75), MAX(attribute76), MAX(attribute77), MAX(attribute78), MAX(attribute79), MAX(attribute80), MAX(attribute81), MAX(attribute82), MAX(attribute83), MAX(attribute84), MAX(attribute85), MAX(attribute86), MAX(attribute87), MAX(attribute88), MAX(attribute89), MAX(attribute90), MAX(attribute91), MAX(attribute92), MAX(attribute93), MAX(attribute94), MAX(attribute95), MAX(attribute96), MAX(attribute97), MAX(attribute98), MAX(attribute99), MAX(attribute100), MAX(attribute101), MAX(attribute102), MAX(attribute103), MAX(attribute104), MAX(attribute105), MAX(attribute106), MAX(attribute107), MAX(attribute108), MAX(attribute109), MAX(attribute110), MAX(attribute111), MAX(attribute112), MAX(attribute113), MAX(attribute114), MAX(attribute115), MAX(attribute116), MAX(attribute117), MAX(attribute118), MAX(attribute119), MAX(attribute120), MAX(attribute121), MAX(attribute122), MAX(attribute123), MAX(attribute124), MAX(attribute125), MAX(attribute126), MAX(attribute127), MAX(attribute128), MAX(attribute129), MAX(attribute130), MAX(attribute131), MAX(attribute132), MAX(attribute133), MAX(attribute134), MAX(attribute135), MAX(attribute136), MAX(attribute137), MAX(attribute138), MAX(attribute139), MAX(attribute140), MAX(attribute141), MAX(attribute142), MAX(attribute143), MAX(attribute144), MAX(attribute145), MAX(attribute146), MAX(attribute147), MAX(attribute148), MAX(attribute149), MAX(attribute150), MAX(attribute151), MAX(attribute152), MAX(attribute153), MAX(attribute154), MAX(attribute155), MAX(attribute156), MAX(attribute157), MAX(attribute158), MAX(attribute159), MAX(attribute160), MAX(attribute161), MAX(attribute162), MAX(attribute163), MAX(attribute164), MAX(attribute165), MAX(attribute166), MAX(attribute167), MAX(attribute168), MAX(attribute169), MAX(attribute170), MAX(attribute171), MAX(attribute172), MAX(attribute173), MAX(attribute174), MAX(attribute175), MAX(attribute176), MAX(attribute177), MAX(attribute178), MAX(attribute179), MAX(attribute180), MAX(attribute181), MAX(attribute182), MAX(attribute183), MAX(attribute184), MAX(attribute185), MAX(attribute186), MAX(attribute187), MAX(attribute188), MAX(attribute189), MAX(attribute190), MAX(attribute191), MAX(attribute192), MAX(attribute193), MAX(attribute194), MAX(attribute195), MAX(attribute196), MAX(attribute197), MAX(attribute198), MAX(attribute199)
         FROM bench
        WHERE attribute0 = 'aaaaaaaaaaaaaaaa'
";
    let mut res = 0;
    let p = 100;
    let mut s = 0.0;
    let mut c = 0;
    let mut ts = Instant::now();

    let statement = ctx.state().sql_to_statement(&query, "Generic").unwrap();
    let plan = ctx.state().statement_to_plan(statement).await.unwrap();
    println!("logical plan = {}", plan);

    let exec_plan = ctx.state().create_physical_plan(&plan).await.unwrap();

    {
        // EXPLAIN.
        let explain_query = format!("EXPLAIN {}", query);
        println!(
            "explained:\n{:?}",
            ctx.sql(&explain_query)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        );
    }

    for i in 0..1000 {
        if i % p == 0 && i > 0 {
            let rps = (p as f64) / (ts.elapsed().as_secs() as f64);
            println!("RPS = {}", rps);
            s += rps;
            c += 1;
            ts = Instant::now();
        }
        res += collect_exec_plan(Arc::clone(&exec_plan), &ctx)
            .await
            .unwrap()
            .len();
    }

    println!("res = {}", res);
    println!("mean = {}", s / (c as f64));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{arrow::{array::{Array, Int32Array, RecordBatch}, datatypes::{Field, Schema, SchemaRef}}, physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner}, prelude::SessionContext};

    #[tokio::test]
    async fn execution_plan_creation_bench() {
        let ctx = SessionContext::new();
        let mut fields = vec![];
        let mut columns = vec![];
        const COLUMNS_NUM: usize = 200;

        for i in 0..COLUMNS_NUM {
            fields.push(Field::new(format!("attribute{}", i), datafusion::arrow::datatypes::DataType::Int32, true));
            columns.push(Int32Array::from(vec![1]));
        }

        let batch = RecordBatch::try_new(
            SchemaRef::new(Schema::new(fields)),
            columns
                .into_iter()
                .map(|it| Arc::new(it) as Arc<dyn Array>)
                .collect(),
        )
        .unwrap();

        ctx.register_batch("t", batch).unwrap();

        let mut aggregates = String::new();
        for i in 0..COLUMNS_NUM {
            if i > 0 {
                aggregates.push_str(", ");
            }
            aggregates.push_str(format!("MAX(attribute{})", i).as_str());
        }

        /*
            SELECT max(attr0), ..., max(attrN) FROM t
        */
        let query = format!("SELECT {} FROM t", aggregates);
        let mut res = 0;

        let statement = ctx.state().sql_to_statement(&query, "Generic").unwrap();
        let plan = ctx.state().statement_to_plan(statement).await.unwrap();
        let planner = DefaultPhysicalPlanner::default();

        let mut duration_sum = 0.0;
        let mut durtaion_count = 0;

        for _ in 0..500 {
            let creation_start = std::time::Instant::now();
            let phys_plan = planner
                .create_physical_plan(&plan, &ctx.state())
                .await
                .unwrap();
            duration_sum += creation_start.elapsed().as_micros() as f64;
            durtaion_count += 1;
            res += phys_plan.children().len();
            println!("================================");
        }

        println!("mean = {}", duration_sum / (durtaion_count as f64));

        /* Not important. */
        println!("res [not important, just to not discard] = {}", res);
    }
}

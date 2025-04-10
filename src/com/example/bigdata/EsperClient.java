package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import net.datafaker.Faker;
import net.datafaker.fileformats.Format;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;

        if (args.length < 2) {
            noOfRecordsPerSec = 5;
            howLongInSec = 20;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        EPCompiled epCompiled = getEPCompiled(config);

        // Connect to the EPRuntime server and deploy the statement
        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        }
        catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement resultStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "answer");

        // Add a listener to the statement to handle incoming events
        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });

        Faker faker = new Faker();

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            waitToEpoch();
            for (int i = 0; i < noOfRecordsPerSec; i++) {
                Timestamp eTimestamp = faker.date().past(60, TimeUnit.SECONDS);
                eTimestamp.setNanos(0);
                Timestamp iTimestamp = Timestamp.valueOf(LocalDateTime.now().withNano(0));
                String[] ores = {"coal", "iron", "gold", "diamond", "emerald"};
                String record = Format.toJson()
                        .set("ore", () -> ores[faker.number().numberBetween(0, ores.length)])
                        .set("depth", () -> String.valueOf(faker.number().numberBetween(1, 36)))
                        .set("amount", () -> String.valueOf(faker.number().numberBetween(1, 10)))
                        .set("ets", eTimestamp::toString)
                        .set("its", iTimestamp::toString)
                        .build().generate();
                runtime.getEventService().sendEventJson(record, "MinecraftEvent");
            }
        }
    }

    private static EPCompiled getEPCompiled(Configuration config) {
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        try {
//          PRZYKŁAD
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//                    @name('answer') SELECT ore, depth, amount, ets, its
//                          from MinecraftEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 3 sec)""", compilerArgs);
// ZADANIE 1:
//   epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//                    @name('answer') SELECT ore, sum(amount) as sumAmount
//                          from MinecraftEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 60 sec)
//                          group by ore;""", compilerArgs);
// ZADANIE 2:
//   epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//                    @name('answer') SELECT depth, amount, ets, its
//                          from MinecraftEvent(ore = 'diamond' and amount > 6 and depth > 12)#length(1);""", compilerArgs);
// ZADANIE 3
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//                    @name('answer') SELECT ore, depth, amount, ets, its
//                             FROM MinecraftEvent#length(1) as m
//                             where amount / (SELECT avg(amount)
//                                  from MinecraftEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 60 sec)
//                                  where ore = m.ore
//                                  group by ore) >= 1.5;""", compilerArgs);
            // ZADANIE 4
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//
//                    @name('answer')
//                    SELECT ore,
//                           sum(case when depth < 10 then amount else 0 end) as sumAmountHeaven,
//                           sum(case when depth > 20 then amount else 0 end) as sumAmountHell
//                    FROM MinecraftEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 60 sec)
//                    WHERE depth < 10 OR depth > 20
//                    GROUP BY ore;
//                """, compilerArgs);

            // ZADANIE 5
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//
//                    @name('answer') select s[0].ore as ore, s[0].depth as depth, s[0].amount as amount,
//                                 s[0].ets as startEts, e.ets as endEts from
//                                 pattern[ every (s=MinecraftEvent until e=MinecraftEvent(amount > 5 and ore = 'diamond')
//                                 where timer:within(30 seconds))];""", compilerArgs);


            epCompiled = compiler.compile("""
                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);

                    @name('answer') select * from
                                 MinecraftEvent()#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 30 sec)
                                 match_recognize (
                                    measures
                                        x[0].ore as ore,
                                        x[0].depth as depth,
                                        x[0].amount as amount,
                                        x[0].ets as startEts,
                                        diamond.ets as endEts
                                    after match skip past last row
                                    pattern (x+ diamond)
                                    define
                                         diamond as diamond.amount > 5 and diamond.ore = 'diamond'
                                    );""", compilerArgs);

            // ZADANIE 6
//                        epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//
//                    @name('answer') select * from MinecraftEvent
//                                                    match_recognize (
//                                                    measures
//                                                        a.ore as ore,
//                                                        a.amount as amount1,
//                                                        b.amount as amount2,
//                                                        c.amount as amount3
//                                                    pattern ( a b c )
//                                                    define
//                                                         a as a.amount > 5,
//                                                         b as b.amount > a.amount and b.ore = a.ore,
//                                                         c as c.amount > b.amount and c.ore = a.ore
//                                                    )""", compilerArgs);

// ZADANIE 7
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MinecraftEvent(ore string, depth int, amount int, ets string, its string);
//
//                                @name('answer') select *
//                                    from MinecraftEvent
//                                    match_recognize (
//                                    measures
//                                        f1.amount + f2.amount + f3.amount + s1.amount + s2.amount + s3.amount as sumAmount,
//                                        f1.its as startIts,
//                                        s3.its as endIts
//                                    pattern (f1 f2 f3 s1 s2 s3)
//                                    define
//                                        f2 as f2.ore != f1.ore,
//                                        f3 as (f3.ore != f2.ore and f3.ore != f1.ore),
//                                        s2 as s2.ore != s1.ore,
//                                        s3 as (s3.ore != s2.ore and s3.ore != s1.ore)
//                                    )""", compilerArgs);

        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }
        return epCompiled;
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTruncated = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTruncated.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}


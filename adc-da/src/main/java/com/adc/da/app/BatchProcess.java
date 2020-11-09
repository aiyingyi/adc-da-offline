package com.adc.da.app;


import com.adc.da.util.ComUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * flink 批处理，用于文件合并排序
 */
public class BatchProcess {
    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = ComUtil.initEnvironment();
        env.setParallelism(1);
        env.addSource(new ExcelSource("C:\\Users\\13099\\Desktop\\1")).print();

    }
}


class ExcelSource extends RichSourceFunction<Map<String, String>> {


    private String filePath = null;

    public ExcelSource(String filePath) {
        this.filePath = filePath;
    }

    public static void parse(File file, SourceContext<Map<String, String>> ctx) {

        File[] fs = file.listFiles();
        for (File f : fs) {
            if (f.isDirectory())    //若是目录，则递归打印该目录下的文件
                parse(f, ctx);
            if (f.isFile()) {
                List<Map<String, String>> res = FileParse.parseVehicleData(f);
                res.forEach(ele -> ctx.collect(ele));
            }
        }
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        parse(new File(filePath), ctx);
    }

    @Override
    public void cancel() {

    }
}


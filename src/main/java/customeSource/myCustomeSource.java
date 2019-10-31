package customeSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zhangyide
 * @desc
 * @date 2019/4/1120:17
 */
public class myCustomeSource implements SourceFunction<Long> {

    private Boolean isRunning=true;
    private Long count=0L;
	@Override
	public void run(SourceContext<Long> sourceContext) throws Exception {

		while (isRunning){
			sourceContext.collect(count);
			count++;
		}
	}

	@Override
	public void cancel() {
		isRunning=false;
	}
}

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import org.apache.flink.util.OutputTag;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * @Auther: songfei20
 * @Date: 2020/12/17 16:35
 * @Description:
 */
public class DateChangeWindowTest {
	private static final Format sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final Logger LOG = LoggerFactory.getLogger(DateChangeWindowTest.class);

	private static final long triggerMeters = 2 * 1000L;


	//TODO 思考
	// 1. 动态窗口		  （适用于有限数据批处理、类似分页，需要动态改变最后一页分页大小）
	// 2. trigger + timer （适用于数据流存在、但跨多个窗口时间无新数据）  OK
	// 3. 2 phase commit  确保数据到齐后、sink成功
	public static void main(String[] args) throws Exception {

		tryWithSideOutput();
	}

	private static void testWithTriggerAndTimer() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);


		OutputTag<Long> lateDataOutput = new OutputTag<Long>("DELTA_LATE_OUTPUT", TypeInformation.of(Long.class));

		MyDeltaTrigger<Long, Window> deltaTrigger = MyDeltaTrigger.of(triggerMeters,
			new DeltaFunction<Long>() {
				private static final long serialVersionUID = 1L;

				@Override
				public double getDelta(
					Long oldDataPoint,
					Long newDataPoint) {
					//数据乱序，差值出现负值不会执行Trigger、也不会输出到OutputTag,目前是通过timer进行强制输出
					return Double.valueOf(newDataPoint - oldDataPoint);
				}
			}, LongSerializer.INSTANCE);

		//f0:	f1: 时间戳
		// 使用窗口作为主逻辑的时候、需要state缓存大量的数据集、在窗口到期的时候
		DataStream<Tuple2<String, Long>> stream = env.addSource(new MyRandomDataSource());

		SingleOutputStreamOperator<Long> singleOutputStreamOperator = stream
			.map((MapFunction<Tuple2<String, Long>, Long>) value -> value.f1)
			//TODO 数据乱序、如何处理
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Long>(Time.seconds(3L)) {
				@Override
				public long extractTimestamp(Long element) {
					return element;
				}
			})

			;

		//TODO 问题：
		// 1. 当没有新数据、Delta一直达不到阈值，可能出现丢数据情况
		// 2. WindowOperator会保存所有数据流中的元素到state中，当窗口过大会导致占用大量的state存储空间。同时在窗口结束、触发计算的时候，海量数据不仅会影响state的查询性能、并且会给下游TPS带来流量高峰。



//			.assignTimestampsAndWatermarks(new MyPunctuatedWatermarks())
		singleOutputStreamOperator.windowAll(GlobalWindows.create()) //模拟时间窗口
			.trigger(PurgingTrigger.of(deltaTrigger))
			.allowedLateness(Time.seconds(5L))
			.sideOutputLateData(lateDataOutput)
			.process(new ProcessAllWindowFunction<Long, String, GlobalWindow>() {
				@Override
				public void process(
					Context context,
					Iterable<Long> elements,
					Collector<String> out) throws Exception {
					LOG.warn("========== Delta data ==========");
					Iterator<Long> iterator = elements.iterator();
					List<Long> dataList = new ArrayList<>();
					while(iterator.hasNext()){
						dataList.add(iterator.next());
					}
					Optional<String> reduce = dataList
						.stream()
						.map(element -> sdf.format(element))
						.reduce((a, b) -> a + "|" + b);
					out.collect(reduce.get());
				}
			})
			.addSink(new SinkFunction<String>() {
				@Override
				public void invoke(String value, Context context) throws Exception {
					LOG.warn("Sink Data: {}" , value);
				}
			});

		singleOutputStreamOperator.getSideOutput(lateDataOutput).addSink(new SinkFunction<Long>() {
			@Override
			public void invoke(Long value) throws Exception {
				LOG.warn("Late element time : {}" , sdf.format(value));
			}
		});




		env.execute();
	}

	private static void tryWithSideOutput() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);


		OutputTag<Long> lateDataOutput = new OutputTag<Long>("LATE_EVENT_OUTPUT"){};

		//f0:	f1: 时间戳
		// 使用窗口作为主逻辑的时候、需要state缓存大量的数据集、在窗口到期的时候
		DataStream<Tuple2<String, Long>> stream = env.addSource(new MyRandomDataSource());

		SingleOutputStreamOperator<Long> sourceStream = stream
			.assignTimestampsAndWatermarks(new MyPunctuatedWatermarks())
			.map((MapFunction<Tuple2<String, Long>, Long>) value -> value.f1)
			;

		SingleOutputStreamOperator<String> windowStream = sourceStream

			//GlobalWindow是一种【伪窗口】实现、内部并无窗口划分，也不属于eventTime语义，不支持Lateness和Sideoutput机制
			.windowAll(TumblingEventTimeWindows.of(Time.seconds(2L)))
			.trigger(EventTimeTrigger.create())
			.allowedLateness(Time.seconds(1L))
			.sideOutputLateData(lateDataOutput)
			.process(new ProcessAllWindowFunction<Long, String, TimeWindow>() {
				@Override
				public void process(
					Context context,
					Iterable<Long> elements,
					Collector<String> out) throws Exception {
					TimeWindow window = context.window();
					LOG.warn(
						"========== Window[{} - {}] trigger! ==========",
						sdf.format(window.getStart()),
						sdf.format(window.getEnd()));

					Iterator<Long> iterator = elements.iterator();
					List<Long> dataList = new ArrayList<>();
					while (iterator.hasNext()) {
						dataList.add(iterator.next());
					}
					Optional<String> reduce = dataList
						.stream()
						.map(element -> sdf.format(element))
						.reduce((a, b) -> a + "|" + b);

					out.collect(reduce.get());
				}
			});

		windowStream.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value, Context context) throws Exception {
				LOG.warn("Window Sink Data: {}", value);
			}
		});

		//TODO sideoutput 无数据
		DataStream<Long> sideOutput = windowStream.getSideOutput(lateDataOutput);
		sideOutput.addSink(new SinkFunction<Long>() {
			@Override
			public void invoke(Long value, Context context) throws Exception {
				LOG.warn("SideOutput Sink Data: {}" , sdf.format(value));
			}
		});

		env.execute("Test with sideoutput");


	}




	/**
	 * Parallel data source that serves a list of key-value pairs.
	 */
	private static class MyDataSource extends RichParallelSourceFunction<Tuple2<String, Long>> {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

			long count = 0L;
			final long numElements = 10 * 1000L + 1;
			LocalDate today = LocalDate.now();
			long val = today.atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();

			List<String> mockDatas = new ArrayList<>();

			while (running) {
				mockDatas.add(sdf.format(val));
				ctx.collect(new Tuple2<>(UUID.randomUUID().toString(), val));

				count += 1000;
				val += 1000;


				//模拟一个窗口无数据
				if(count > numElements){
					TimeUnit.MILLISECONDS.sleep(triggerMeters * 2);
					running = false;
				}

			}

			LOG.warn("Mock data : {}" , String.join("###",mockDatas));
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class MyRandomDataSource extends RichParallelSourceFunction<Tuple2<String, Long>> {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

			long count = 0L;
			final long numElements = 10 * 1000L + 1;
			LocalDate today = LocalDate.now();
			long start = today.atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();

			List<Long> mockDatas = new ArrayList<>();

			Random rnd = new Random();
			long val = start;

			while (running) {
				mockDatas.add(val);

				ctx.collect(new Tuple2<>(UUID.randomUUID().toString(), val));
				count += 1000;
				val = start + rnd.nextInt(10) * 1000L;

				//模拟Lateness
				/*if((val % 10) % 5 == 0){
					long now = System.currentTimeMillis();
					ctx.collect(new Tuple2<>(UUID.randomUUID().toString(),now));
					mockDatas.add(now);
				}*/

				//模拟一个窗口无数据
				if(count > numElements){
					running = false;
				}

				TimeUnit.MILLISECONDS.sleep(10L);
			}
			Optional<String> stringOptional = mockDatas
				.stream()
				.map(e -> sdf.format(e))
				.reduce((a, b) -> a + "||" + b);

			LOG.warn("Mock data : {}" , stringOptional.get());
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class MyPeriodWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>> {

		private long currentTimestamp = Long.MIN_VALUE;

		@Override
		public Watermark getCurrentWatermark() {
			LOG.warn("Period watermark was created !");
			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp);
		}

		@Override
		public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
			long timestamp = element.f1;
			currentTimestamp = Math.max(timestamp, currentTimestamp);
			return timestamp;
		}
	}

	private static class MyPunctuatedWatermarks implements AssignerWithPunctuatedWatermarks<Tuple2<String,Long>> {

		private Long currentTimestamp = Long.MIN_VALUE;


		@Override
		public Watermark checkAndGetNextWatermark(
			Tuple2<String, Long> lastElement,
			long extractedTimestamp) {
			return new Watermark(extractedTimestamp );
		}

		@Override
		public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
			return element.f1;
		}
	}


	private static class MyPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

		/** The current maximum timestamp seen so far. */
		private long currentMaxTimestamp = Long.MIN_VALUE;

		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentMaxTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentMaxTimestamp - 1);
		}

		@Override
		public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
			long elementTime = element.f1;

			if(elementTime >= currentMaxTimestamp){
				currentMaxTimestamp = elementTime;
			}
			return elementTime;
		}
	}


	private static class MyDeltaTrigger<T, W extends Window> extends Trigger<T,W> {


		private final DeltaFunction<T> deltaFunction;
		private final double threshold;
		private final ValueStateDescriptor<T> stateDesc;

		private MyDeltaTrigger(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
			this.deltaFunction = deltaFunction;
			this.threshold = threshold;
			stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);

		}

		@Override
		public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
			ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);

			//Felix: lastElementState为作业启动后、首次进入当前窗口的元素
			if (lastElementState.value() == null) {
				lastElementState.update(element);
				//此处注册Timer、避免第一个窗口内一直无数据
				ctx.registerEventTimeTimer((long)(ctx.getCurrentProcessingTime() + this.threshold));
				return TriggerResult.CONTINUE;
			}
			//Felix: 当最新元素和窗口首元素差值超过阈值、将会触发一次窗口计算
			if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
				lastElementState.update(element);
				//此处注册Timer、避免下一个窗口内一直无数据
				ctx.registerEventTimeTimer((long)(ctx.getCurrentProcessingTime() + this.threshold));
				return TriggerResult.FIRE;
			}
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
			LOG.warn("EventTimeTimer is called at :{}" , sdf.format(time));
			return TriggerResult.FIRE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}

		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {
			ctx.getPartitionedState(stateDesc).clear();
		}

		@Override
		public String toString() {
			return "DeltaTrigger(" +  deltaFunction + ", " + threshold + ")";
		}

		/**
		 * Creates a delta trigger from the given threshold and {@code DeltaFunction}.
		 *
		 * @param threshold The threshold at which to trigger.
		 * @param deltaFunction The delta function to use
		 * @param stateSerializer TypeSerializer for the data elements.
		 *
		 * @param <T> The type of elements on which this trigger can operate.
		 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
		 */
		public static <T, W extends Window> MyDeltaTrigger<T, W> of(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
			return new MyDeltaTrigger<>(threshold, deltaFunction, stateSerializer);
		}
	}
}

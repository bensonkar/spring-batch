package com.example.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;
import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchApplication {

    public static String[] names = new String[]{"orderId", "firstName", "lastName", "email", "cost", "itemId",
            "itemName", "shipDate"};
    public static String[] tokens = {"order_id", "first_name", "lastName", "email", "cost", "item_id", "item_name", "ship_date"};
    public static String INSERT_ORDER_SQL = "insert into "
            + "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
            + " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate)";

    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    @Autowired
    public DataSource datasource;

    public static String ORDER_SQL = "select order_id, first_name, last_name, "
            + "email, cost, item_id, item_name, ship_date "
            + "from SHIPPED_ORDER order by order_id";

    @Bean
    public ItemProcessor<Order, Order> trackedOrderItemProcessor() {
        return null;
    }

    @Bean
    public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
        BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<>();
        itemProcessor.setFilter(true);
        return itemProcessor;
    }

    @Bean
    public ItemWriter<Order> itemWriter() {
        // reading data from database and writing to csv file
        /*FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource("C:\\Users\\A242932\\Downloads\\output_data.csv"));

        DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");

        BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(names);

        aggregator.setFieldExtractor(fieldExtractor);

        itemWriter.setLineAggregator(aggregator);
        return itemWriter;*/

        //inserting data to database
       /* return new JdbcBatchItemWriterBuilder<Order>()
                .dataSource(datasource)
                .sql(INSERT_ORDER_SQL)
//                .itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
                .beanMapped()
                .build();*/

        return new JsonFileItemWriterBuilder<Order>()
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<Order>())
                .resource(new FileSystemResource("c:\\Users\\A242932\\Downloads\\shipped-_orders_output.json"))
                .name("jsonItemWriter")
                .build();
    }

    @Bean
    public PagingQueryProvider queryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();

        factory.setSelectClause("select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date");
        factory.setFromClause("from SHIPPED_ORDER");
        factory.setSortKey("order_id");
        factory.setDataSource(datasource);
        return factory.getObject();
    }

    @Bean
    public ItemReader<Order> itemReader() throws Exception {
        //reading data from csv file
        /*FlatFileItemReader<Order> itemReader = new FlatFileItemReader<>();
        itemReader.setLinesToSkip(1);
        itemReader.setResource(new FileSystemResource("C:\\Users\\A242932\\Downloads\\Ex_Files_Spring_Spring_Batch\\Ex_Files_Spring_Spring_Batch\\Exercise Files\\data\\shipped_orders.csv"));

        DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(tokens);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new OrderFieldMapper());

        itemReader.setLineMapper(lineMapper);
        return itemReader;*/

        return new JdbcPagingItemReaderBuilder<>()
                .dataSource(datasource)
                .name("jdbcCursorItemReader")
                .queryProvider(queryProvider())
                .rowMapper(new OrderRowMapper())
                .pageSize(10)
                .build();
    }

    @Bean
    public Step chunkBasedStep() throws Exception {
        return stepBuilderFactory.get("chunkBasedStep")
//                .<Order, Order>chunk(10)
                .<Order, TrackedOrder>chunk(10)
                .reader(itemReader())
//                .writer(new ItemWriter<Order>() {
//                    @Override
//                    public void write(List<? extends Order> list) throws Exception {
//                        System.out.println("Received list of size:  "+list.size());
//                        list.forEach(System.out::println);
//                    }
//                }).build();
//                .processor(orderValidatingItemProcessor())
//                .processor(trackedOrderItemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("job")
                .start(chunkBasedStep())
                .build();
    }

    @Bean
    public StepExecutionListener selectFlowerListener() {
        return new FlowersSelectionStepExecutionListener();
    }

    @Bean
    public JobExecutionDecider decider() {
        return new DeliveryDecider();
    }

    @Bean
    public JobExecutionDecider receiptDecider() {
        return new ReceiptDecider();
    }

    @Bean
    public Step nestedBillingJobStep() {
        return stepBuilderFactory.get("nestedBillingJobStep").job(billingJob()).build();
    }

    @Bean
    public Step sendInvoiceStep() {
        return stepBuilderFactory.get("sendInvoiceStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Sending invoice to customer.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Flow billingFlow() {
        return new FlowBuilder<SimpleFlow>("billingFlow").start(sendInvoiceStep()).build();
    }

    @Bean
    public Job billingJob() {
        return jobBuilderFactory.get("billingJob").start(sendInvoiceStep()).build();
    }

    @Bean
    public Flow deliveryFlow() {
        return new FlowBuilder<SimpleFlow>("deliveryFlow")
                .start(driveToAddressStep())
                .on("FAILED").fail()
                .from(driveToAddressStep())
                .on("*").to(decider())
                .on("PRESENT").to(givePackageToCustomerStep())
                .next(receiptDecider()).on("CORRECT").to(thankCustomerStep())
                .from(receiptDecider()).on("INCORRECT").to(refundStep())
                .from(decider()).on("NOT PRESENT").to(leaveAtDoorStep())
                .build();
    }

    @Bean
    public Step selectFlowerStep() {
        return stepBuilderFactory.get("selectFlowerStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Gathering flowers for order.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step removeThornsStep() {
        return stepBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Remove thorns from roses");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step arrangeFlowersStep() {
        return stepBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Arranging flowers for order.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

//    @Bean
//    public Job prepareFlowers() {
//        return jobBuilderFactory.get("prepareFlowersJob")
//                .start(selectFlowerStep())
//                .on("TRIM_REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
//                .from(selectFlowerStep()).on("NO_TRIM_REQUIRED").to(arrangeFlowersStep())
//                .from(arrangeFlowersStep()).on("*").to(deliveryFlow())
//                .end()
//                .build();
//    }

    @Bean
    public Step thankCustomerStep() {
        return stepBuilderFactory.get("thankCustomerStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("we are giving thanks to our customer");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step refundStep() {
        return stepBuilderFactory.get("refundStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("we are refunding customer money.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step leaveAtDoorStep() {
        return stepBuilderFactory.get("leaveAtDoorStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("we are leaving package at the door.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step storePackageStep() {
        return stepBuilderFactory.get("storePackageStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("we are storing customer where the customer address is located.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step givePackageToCustomerStep() {
        return stepBuilderFactory.get("givePackageToCustomerStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("we have given package to customer.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step driveToAddressStep() {
        boolean GOT_LOST = false;
        return stepBuilderFactory.get("driveToAddressStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                if (GOT_LOST) {
                    throw new RuntimeException("Got lost driving to the address.");
                }
                System.out.println("successfully arrived at the address");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step packageItemStep() {
        return stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
//                String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
//                String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();
//                System.out.println(String.format("The %s has been packaged on %s.",item,date));
                System.out.println("The item has been packaged.");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Job deliverPackageJob() {
        return jobBuilderFactory
                .get("deliverPackageJob")
                .start(packageItemStep())
                .split(new SimpleAsyncTaskExecutor())
                .add(deliveryFlow(), billingFlow())
                .end()
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchApplication.class, args);
    }

}

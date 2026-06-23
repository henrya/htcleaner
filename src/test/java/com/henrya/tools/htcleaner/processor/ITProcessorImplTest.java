package com.henrya.tools.htcleaner.processor;

import com.henrya.tools.htcleaner.Cleaner;
import com.henrya.tools.htcleaner.driver.CleanerDriverImpl;
import com.henrya.tools.htcleaner.exception.CleanerException;
import com.henrya.tools.htcleaner.tools.DataCreator;
import com.henrya.tools.htcleaner.tools.TestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ITProcessorImplTest {

    @BeforeEach
    void setUp() {
        DataCreator.createData(TestConfig.TABLE_NAME, 9999);
        DataCreator.createData(TestConfig.TABLE_NAME + "_WHERE", 9999);
        DataCreator.createCompositeData(TestConfig.TABLE_NAME + "_COMPOSITE", 12);
    }

    @AfterEach
    void tearDown() {
        DataCreator.executeUpdate("DROP TABLE IF EXISTS " + TestConfig.TABLE_NAME);
        DataCreator.executeUpdate("DROP TABLE IF EXISTS " + TestConfig.TABLE_NAME + "_WHERE");
        DataCreator.executeUpdate("DROP TABLE IF EXISTS " + TestConfig.TABLE_NAME + "_COMPOSITE");

    }

    @Test
    @DisplayName("Test processor with all-rows predicate")
    void processTest() {
        try (CleanerDriverImpl cleanerDriver = new CleanerDriverImpl(TestConfig.getCleaner().getDriver())) {
            ProcessorImpl processor = new ProcessorImpl();
            Cleaner cleaner = TestConfig.getCleaner();
            cleaner.setLimit(500);
            cleaner.setSleep(10);
            cleaner.setProgressDelay(100);
            processor.process(cleaner, cleanerDriver);
            assertThat(DataCreator.executeCount(TestConfig.TABLE_NAME, null)).isZero();
        } catch (CleanerException e) {
            Assertions.fail(e);
        }
    }

    @Test
    @DisplayName("Test processor with where")
    void processWithWhereTest() {
        try (CleanerDriverImpl cleanerDriver = new CleanerDriverImpl(TestConfig.getCleaner().getDriver())) {
            ProcessorImpl processor = new ProcessorImpl();
            Cleaner cleaner = TestConfig.getCleaner();
            cleaner.setTable(TestConfig.TABLE_NAME + "_WHERE");
            cleaner.setWhere("c < 5");
            cleaner.setQuiet(true);
            cleaner.setLimit(2000);
            processor.process(cleaner, cleanerDriver);
            assertThat(DataCreator.executeCount(TestConfig.TABLE_NAME + "_WHERE", null)).isEqualTo(9994);
            assertThat(DataCreator.executeCount(TestConfig.TABLE_NAME + "_WHERE", "c < 5")).isZero();
        } catch (CleanerException e) {
            Assertions.fail(e);
        }
    }

    @Test
    @DisplayName("Dry run does not delete rows")
    void processDryRunTest() {
        try (CleanerDriverImpl cleanerDriver = new CleanerDriverImpl(TestConfig.getCleaner().getDriver())) {
            ProcessorImpl processor = new ProcessorImpl();
            Cleaner cleaner = TestConfig.getCleaner();
            cleaner.setQuiet(true);
            cleaner.setDryRun(true);
            cleaner.setLimit(5);
            processor.process(cleaner, cleanerDriver);
            assertThat(DataCreator.executeCount(TestConfig.TABLE_NAME, null)).isEqualTo(9999);
        } catch (CleanerException e) {
            Assertions.fail(e);
        }
    }

    @Test
    @DisplayName("Test processor with composite primary key")
    void processWithCompositePrimaryKeyTest() {
        try (CleanerDriverImpl cleanerDriver = new CleanerDriverImpl(TestConfig.getCleaner().getDriver())) {
            ProcessorImpl processor = new ProcessorImpl();
            Cleaner cleaner = TestConfig.getCleaner();
            cleaner.setTable(TestConfig.TABLE_NAME + "_COMPOSITE");
            cleaner.setQuiet(true);
            cleaner.setLimit(5);
            processor.process(cleaner, cleanerDriver);
            assertThat(DataCreator.executeCount(TestConfig.TABLE_NAME + "_COMPOSITE", null)).isZero();
            assertThat(cleaner.getPrimaryKey()).isNull();
        } catch (CleanerException e) {
            Assertions.fail(e);
        }
    }
}

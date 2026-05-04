from schedule_holiday import check_date_exists
from runtime.nutterfixture import NutterFixture
from pyspark.sql import Row
from datetime import datetime

class DateCheckFixture(NutterFixture):
    def __init__(self):
        super().__init__() 
        self.result = None

    def before_all(self):
        data = [
            Row(Date="2024-03-29"),
            Row(Date="2024-05-23"),
            Row(Date="2024-10-12")
        ]
        self.df = spark.createDataFrame(data)

    #--------- Test Case 1---------#
    def run_date_present(self):
        self.result = check_date_exists(self.df, "2024-10-12")

    def assertion_date_present(self):
        assert self.result == True

    #---------- Test case 2---------#
    def run_date_not_present(self):
        self.result = check_date_exists(self.df, "2026-02-01")

    def assertion_date_not_present(self):
        assert self.result == False

result = DateCheckFixture().execute_tests()
print(result.to_string())
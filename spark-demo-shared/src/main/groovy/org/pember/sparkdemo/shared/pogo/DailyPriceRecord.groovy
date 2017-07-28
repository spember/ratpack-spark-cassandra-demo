package org.pember.sparkdemo.shared.pogo

import groovy.transform.CompileStatic

import java.text.ParseException
import java.time.LocalDate

@CompileStatic
class DailyPriceRecord implements Serializable {
    String symbol
    LocalDate recordedDate
    double value
    double previousValue
    double valueChange
    double percentChange
    int shareVolume
    int year
    int month
    int day


    static DailyPriceRecord buildFromFileRow(String fileRow) {
        String[] tokens = fileRow.split("\\|")
        assert tokens.length == 7
        DailyPriceRecord record = new DailyPriceRecord()

        record.setSymbol(tokens[0])

        try {
            LocalDate recordedDate = LocalDate.parse(tokens[1])
            
            record.setRecordedDate(recordedDate)
            record.setYear(recordedDate.getYear())
            record.setMonth(recordedDate.getMonthValue())
            record.setDay(recordedDate.getDayOfMonth())
        } catch (ParseException e) {
            e.printStackTrace()
        }

        record.setValue(Double.parseDouble(tokens[2]))
        record.setPreviousValue(Double.parseDouble(tokens[3]))
        record.setValueChange(Double.parseDouble(tokens[4]))
        record.setPercentChange(Double.parseDouble(tokens[5]))
        record.setShareVolume(Integer.parseInt(tokens[6]))
        return record
    }
}

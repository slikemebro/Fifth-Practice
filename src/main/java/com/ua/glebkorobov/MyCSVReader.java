package com.ua.glebkorobov;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class MyCSVReader {

    public List<String[]> readCSVFile(String nameOfFile) {
        try {
            CSVReader reader = new CSVReaderBuilder((new FileReader(nameOfFile))).build();
            return reader.readAll();
        } catch (IOException | CsvException e) {
            throw new RuntimeException(e);
        }
    }
}

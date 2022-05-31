package ru.sbt.sup.jdbc.adapter;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import ru.sbt.sup.jdbc.config.FormatCSVSpec;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class CsvInputStreamParser implements Closeable {

    private final CsvParser parser;
    private final RowConverter converter;

    public CsvInputStreamParser(FormatCSVSpec spec, RowConverter converter, InputStream inputStream) {
        CsvFormat format = new CsvFormat();
        format.setDelimiter(spec.getDelimiter());
        format.setLineSeparator(spec.getLineSeparator());
        format.setQuote(spec.getQuoteChar());
        format.setQuoteEscape(spec.getEscape());
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setFormat(format);
        parserSettings.setHeaderExtractionEnabled(spec.isHeader());
        this.converter = converter;
        this.parser = new CsvParser(parserSettings);
        this.parser.beginParsing(inputStream, StandardCharsets.UTF_8);
    }

    public Optional<Object[]> parseRecord() {
        String[] strings = parser.parseNext();
        if (strings == null) {
            parser.stopParsing();
            return Optional.empty();
        } else {
            Object[] values = converter.convertRow(strings);
            return Optional.of(values);
        }
    }

    @Override
    public void close() {
        if (!parser.getContext().isStopped()) {
            parser.stopParsing();
        }
    }
}

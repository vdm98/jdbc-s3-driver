package ru.sbt.sup.jdbc.adapter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import ru.sbt.sup.jdbc.config.ColumnSpec;
import ru.sbt.sup.jdbc.config.TableSpec;
import ru.sbt.sup.jdbc.config.TypeSpec;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class LakeTable extends AbstractTable implements ProjectableFilterableTable {
    private final TableSpec spec;
    private final ColumnSpec[] columns;
    private final int[] defaultProjects;
    private final AmazonS3 s3Client;

    LakeTable(AmazonS3 s3Client, TableSpec spec) {
        this.s3Client = s3Client;
        this.spec = spec;
        this.columns = spec.columns.toArray(new ColumnSpec[0]);
        this.defaultProjects = IntStream.range(0, columns.length).toArray();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSpec c : columns) {
            RelDataType relDataType = typeFactory.createJavaType(c.datatype.toJavaClass());
            builder.add(c.label.toUpperCase(), relDataType);
            builder.nullable(c.nullable == null || c.nullable);
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        projects = (projects == null) ? defaultProjects : projects;
        AmazonS3URI s3Source = new AmazonS3URI(spec.location);
        TypeSpec[] fields = spec.columns.stream().map(c -> c.datatype).toArray(TypeSpec[]::new);
        LakeS3Adapter scan = new LakeS3Adapter(s3Client, s3Source, spec.format, fields, projects, filters);
        return new AbstractEnumerable<>() {

            public Enumerator<Object[]> enumerator() {

                CsvInputStreamParser parser = new CsvInputStreamParser(
                        scan.getFormat(),
                        scan.getRowConverter(),
                        scan.getS3Result());

                return new Enumerator<>() {
                    private Object[] current;
                    public Object[] current() {
                        return current;
                    }
                    public void reset() {
                        throw new UnsupportedOperationException();
                    }
                    public void close() {
                        parser.close();
                    }
                    public boolean moveNext() {
                        if (cancelFlag.get()) {
                            return false;
                        }
                        Optional<Object[]> result = parser.parseRecord();
                        if (result.isEmpty()) {
                            current = null;
                            return false;
                        }
                        current = result.get();
                        return true;
                    }
                };
            }

        };
    }
}
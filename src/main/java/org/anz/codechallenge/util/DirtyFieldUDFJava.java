package org.anz.codechallenge.util;

import org.anz.codechallenge.schema.JSONSchema;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;
import scala.collection.mutable.Seq;

/**
 *  This class is for demo purpose
 */
public class DirtyFieldUDFJava implements UDF1<Seq<Column>, String> {

    private JSONSchema fileSchema;

    public DirtyFieldUDFJava(JSONSchema fileSchema) {
        this.fileSchema = fileSchema;
    }

    @Override
    public String call(Seq<Column> seqCols) throws Exception {
        boolean dirty = false;
        int i=0;
        /*Iterator<Column> iterator = seqCols.iterator();
        for (FileSchema fileSchema : fileSchema.getColumns()) {
            String colValue = iterator.next().getItem(fileSchema.getName());
            boolean isNull = (colValue == null || colValue.trim().isBlank() || colValue.trim().isEmpty());
            colValue = (isNull) ? colValue : colValue.trim();
            // Mandatory field check
            if (fileSchema.getMandatory() != null && fileSchema.getMandatory().equals("true")) {
                if (isNull) {
                    dirty = true;
                    break;
                }
            }
            // Column type check
            if (fileSchema.getType() != null) {
                if (isNull) {
                    dirty = true;
                    break;
                }
                else {
                    try {
                        switch (fileSchema.getType()) {
                            case "INTEGER":
                                Float.parseFloat(colValue);
                                break;
                            case "FLOAT":
                                Float.parseFloat(colValue);
                                break;
                        }
                    } catch(Exception e) {
                        dirty = true;
                        break;
                    }
                }
            }
            // Format check #ASSUME this is only for DATE type
            if (fileSchema.getFormat() != null && fileSchema.getFormat().equals("DATE")) {
                if (isNull) {
                    dirty = true;
                    break;
                } else {
                    List<String> datesList = new ArrayList<>();
                    datesList.add(fileSchema.getFormat());
                    if(fileSchema.getFormat().equals("dd-MM-yyyy")) {
                        datesList.add("d-MM-yyyy");
                        datesList.add("dd/MM/yy");
                    }
                    List<DateTimeFormatter> dateFormatList = datesList.stream().map(d -> (DateTimeFormatter.ofPattern(d)))
                            .collect(Collectors.toList());

                    dirty = true;

                    for(DateTimeFormatter dateFormat : dateFormatList) {
                        try {
                            dateFormat.parse(colValue);
                            dirty = false;
                            break;

                        } catch(Exception e) {
                        }
                    }
                }
            }
        }*/

        String status = (dirty) ? "1" : "0";
        return status;
    }
}

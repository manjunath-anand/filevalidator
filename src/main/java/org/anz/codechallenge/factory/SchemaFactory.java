package org.anz.codechallenge.factory;

import com.google.gson.Gson;
import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.schema.EmptySchema;
import org.anz.codechallenge.schema.JSONSchema;
import org.anz.codechallenge.schema.Schema;
import org.anz.codechallenge.util.FileUtil;

/**
 * Factory to create Schema
 */
public class SchemaFactory {

    public static Schema getSchema(ContentParams inputContentParams) {
        Schema schema = null;
        // Need to get schema type details from input params of config file
        if(inputContentParams.getSchemaPath() != null) {
            String schemaStr = FileUtil.readAllBytes(inputContentParams.getSchemaPath());
            Gson gson = new Gson();
            schema = gson.fromJson(schemaStr, JSONSchema.class);
        } else {
            schema = EmptySchema.getInstance();
        }
        return schema;
    }

}

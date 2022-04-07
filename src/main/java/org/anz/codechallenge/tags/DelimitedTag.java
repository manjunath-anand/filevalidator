package org.anz.codechallenge.tags;

public class DelimitedTag implements Tag {

    public static final String DEFAULT_DELIMITER = "\\|";
    private final String tagType;
    private final String tagPath;
    private final String delimiter;
    private final String file_name;
    private final int record_count;
    private boolean isEmpty = true;

    public DelimitedTag(String tagType, String tagPath, String delimiter, String file_name, int record_count) {
        this.tagType = tagType;
        this.tagPath = tagPath;
        this.delimiter = delimiter;
        this.file_name = file_name;
        this.record_count = record_count;
        if(tagPath != null && !tagPath.trim().isEmpty()) {
            this.isEmpty = false;
        }
    }

    public String getFile_name() {
        return file_name;
    }

    public int getRecord_count() {
        return record_count;
    }

    public String getTagType() {
        return tagType;
    }

    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public boolean isEmpty() {
        return isEmpty;
    }

    @Override
    public String getTagPath() {
        return tagPath;
    }

}

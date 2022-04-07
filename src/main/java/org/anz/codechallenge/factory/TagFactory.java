package org.anz.codechallenge.factory;

import org.anz.codechallenge.filedetails.ContentParams;
import org.anz.codechallenge.tags.EmptyTag;
import org.anz.codechallenge.tags.DelimitedTag;
import org.anz.codechallenge.tags.Tag;
import org.anz.codechallenge.util.FileUtil;

/**
 * Factory to create Tag
 */
public class TagFactory {
    public static Tag getTag(ContentParams inputContentParams) {
        Tag tag = null;
        // Need to get tag type details from input params of config file
        if(inputContentParams.getTagPath() != null) {
            String tagFileStr = FileUtil.readAllBytes(inputContentParams.getTagPath());
            String[] tagArr = tagFileStr.split(DelimitedTag.DEFAULT_DELIMITER);
            String file_name = tagArr[0];
            int record_count = Integer.valueOf(tagArr[1]);
            tag = new DelimitedTag(Tag.DEFAULT_TYPE,inputContentParams.getTagPath(),"\\|",file_name,record_count);

        } else {
            tag = EmptyTag.getInstance();
        }
        return tag;
    }
}

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

public class BcakUpClearTask extends TimerTask {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
    @Override
    public void run() {
        try {
            Properties props=PropertyHolder.getProps();
            long now =new Date().getTime();
            File backupFile = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR));
            File[] files=backupFile.listFiles();

            for(File file:files)
            {

                    long time =sdf.parse(file.getName()).getTime();
                    if((now - time) > 24*60*60*1000L)
                    {
                        FileUtils.deleteDirectory(file);
                    }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

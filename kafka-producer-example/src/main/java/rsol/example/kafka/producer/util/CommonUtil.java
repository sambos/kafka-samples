package rsol.example.kafka.producer.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

@UtilityClass
@Log4j2
public class CommonUtil {

	
	
	private static final String DATE_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";

	/**
	 * This method is used for returning the Current Date in String!.
	 * 
	 * @return
	 */
	public static String getCurrentDateInString() {
		DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
		Calendar cal = Calendar.getInstance();
		String date = dateFormat.format(cal.getTime());
		log.info(date);
		return date;
	}
}

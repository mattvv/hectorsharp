using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HectorSharp.Utils
{
/**
 * Encoding and decoding utilities.
 *
 * @author Matt Van Veenendaal (m@mattvv.com)
 * @author Ran Tavory (rantav@gmail.com) Original Java Author
 *
 */
public class StringUtils {
 
  //private static sealed Logger log = LoggerFactory.getLogger(StringUtils.class);
 
  public static sealed System.Text.ASCIIEncoding  encoding=new System.Text.ASCIIEncoding();
 
  /**
   * Gets UTF-8 bytes from the string.
   *
   * @param s
   * @return
   */
  public static byte[] bytes(String s) {
    try {
      
      return encoding.GetBytes(s);
    } catch (Exception e) {
      //log.error("UnsupportedEncodingException ", e);
      throw new Exception(e.Message);
    }
  }
 
  /**
   * Utility for converting bytes to strings. UTF-8 is assumed.
   * @param bytes
   * @return
   */
  public static String toString(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      return bytes.ToString();
    } catch (Exception e) {
      //log.error("UnsupportedEncodingException ", e);
      throw new Exception(e.Message);
    }
  }
 
}

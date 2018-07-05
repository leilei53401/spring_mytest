package com.voole.ad.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;



public class BeanAvroUtils {
	
	/** 转化avro格式 **/
	public static <T extends SpecificRecordBase> byte[] toAvro(T info) {
		byte[] byteData = null;
		GenericDatumWriter<T> writer = new GenericDatumWriter<T>(info.getSchema());
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			Encoder e = EncoderFactory.get().binaryEncoder(os, null);
			writer.write(info, e);
			e.flush();
			byteData = os.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				os.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return byteData;
	}

}

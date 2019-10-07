package org.openfeed.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

public class PbUtil {
	private static Printer printer = com.google.protobuf.util.JsonFormat.printer().omittingInsignificantWhitespace();
	private static JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();

	public static String toJson(MessageOrBuilder msg) {
		try {
			return printer.print(msg);
		} catch (InvalidProtocolBufferException ignore) {
		}
		return null;
	}

	public static void decode(String json, Message.Builder builder) {
		try {
			parser.merge(json, builder);
		} catch (InvalidProtocolBufferException e) {
		}
	}

	private PbUtil() {
	}
}

package org.openfeed.client.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.openfeed.Service;
import org.openfeed.SubscriptionType;
import org.openfeed.client.api.InstrumentCache;
import org.openfeed.client.api.OpenfeedClientEventHandler;
import org.openfeed.client.api.OpenfeedClientHandler;
import org.openfeed.client.api.impl.ConnectionStats;
import org.openfeed.client.api.impl.InstrumentCacheImpl;
import org.openfeed.client.api.impl.OpenfeedClientConfigImpl;
import org.openfeed.client.api.impl.websocket.OpenfeedClientWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample Openfeed Client using Google Protobuf
 *
 */
public class OpenfeedClientExampleMain {
    private static final Logger log = LoggerFactory.getLogger(OpenfeedClientExampleMain.class);
    private static Options options;
    private InstrumentCache instrumentCache = new InstrumentCacheImpl();
    private OpenfeedClientConfigImpl config;

    public static void main(String[] args) throws Exception {
        OpenfeedClientConfigImpl config = new OpenfeedClientConfigImpl();
        options = new Options();
        options.addOption(Option.builder("u").hasArg().required().desc("user name").build());
        options.addOption(Option.builder("p").hasArg().required().desc("password").build());
        // Subscriptions
        options.addOption(Option.builder("s").hasArg().desc("Symbol(s) to subscribe too, comma separated.").build());
        options.addOption(
                Option.builder("ids").hasArg().desc("Openfeed Id(s) to subscribe too, comma separated.").build());
        options.addOption(Option.builder("e").hasArg().desc("Exchange(s) to subscribe too, comma separated.").build());
        options.addOption(Option.builder("chids").hasArg()
                .desc("Openfeed Chanel Id(s) to subscribe too, comma separated.").build());
        // Types
        options.addOption(Option.builder("qp").desc("Quote Participant Subscription").build());
        options.addOption(Option.builder("t").desc("Trades Subscription").build());
        // Instruments
        options.addOption(Option.builder("ir").desc("instrument request, requires -s,-e or -ids").build());
        options.addOption(
                Option.builder("irx").desc("instrument cross reference request, requires -s,-e or -ids").build());
        //
        options.addOption(Option.builder("host").hasArg().desc("Host, defaults " + config.getHost()).build());
        options.addOption(Option.builder("port").hasArg().desc("Port, defaults " + config.getPort()).build());
        //
        options.addOption(Option.builder("lh").desc("log heartbeats").build());
        options.addOption(Option.builder("li").desc("log instruments").build());
        options.addOption(Option.builder("ls").desc("log snapshots").build());
        options.addOption(Option.builder("lu").desc("log updates").build());
        options.addOption(Option.builder("lt").desc("log trades").build());
        options.addOption(Option.builder("ltc").desc("log trade cancel").build());
        options.addOption(Option.builder("ltco").desc("log trade correction").build());
        //
        options.addOption(Option.builder("h").desc("help").build());
        CommandLineParser cmdParser = new org.apache.commons.cli.DefaultParser();
        CommandLine cmdLine;
        try {
            cmdLine = cmdParser.parse(options, args);
        } catch (ParseException e) {
            log.error("Cmd line error: {}", e.getMessage());
            printHelp();
            return;
        }
        String v = null;
        if (cmdLine.hasOption("u")) {
            config.setUserName(cmdLine.getOptionValue("u"));
        }
        if (cmdLine.hasOption("p")) {
            config.setPassword(cmdLine.getOptionValue("p"));
        }
        if (cmdLine.hasOption("s")) {
            v = cmdLine.getOptionValue("s");
            String[] syms = v.split(",");
            config.setSymbols(syms);
        }
        if (cmdLine.hasOption("ids")) {
            v = cmdLine.getOptionValue("ids");
            String[] vs = v.split(",");
            long[] ids = new long[vs.length];
            for (int i = 0; i < vs.length; i++) {
                ids[i] = (Long.parseLong(vs[i]));
            }
            config.setMarketIds(ids);
        }
        if (cmdLine.hasOption("e")) {
            v = cmdLine.getOptionValue("e");
            String[] vs = v.split(",");
            String[] exchanges = new String[vs.length];
            for (int i = 0; i < vs.length; i++) {
                exchanges[i] = vs[i];
            }
            config.setExchanges(exchanges);
        }
        if (cmdLine.hasOption("chids")) {
            v = cmdLine.getOptionValue("chids");
            String[] vs = v.split(",");
            int[] ids = new int[vs.length];
            for (int i = 0; i < vs.length; i++) {
                ids[i] = (Integer.parseInt(vs[i]));
            }
            config.setChannelIds(ids);
        }
        if (cmdLine.hasOption("qp")) {
            config.setSubscriptonType(SubscriptionType.QUOTE_PARTICIPANT);
        }
        if (cmdLine.hasOption("t")) {
            config.setSubscriptonType(SubscriptionType.TRADES);
        }
        if (cmdLine.hasOption("ir")) {
            config.setInstrumentRequest(true);
        }
        if (cmdLine.hasOption("irx")) {
            config.setInstrumentCrossReferenceRequest(true);
        }
        if (cmdLine.hasOption("host")) {
            config.setHost(cmdLine.getOptionValue("host"));
        }
        if (cmdLine.hasOption("port")) {
            config.setPort(Integer.parseInt(cmdLine.getOptionValue("port")));
        }
        if (cmdLine.hasOption("lh")) {
            config.setLogHeartBeat(true);
        }
        if (cmdLine.hasOption("li")) {
            config.setLogInstrument(true);
        }
        if (cmdLine.hasOption("ls")) {
            config.setLogSnapshot(true);
        }
        if (cmdLine.hasOption("lu")) {
            config.setLogUpdates(true);
        }
        if (cmdLine.hasOption("lt")) {
            config.setLogTrade(true);
        }
        if (cmdLine.hasOption("ltc")) {
            config.setLogTradeCancel(true);
        }
        if (cmdLine.hasOption("ltco")) {
            config.setLogTradeCorrection(true);
        }
        if (cmdLine.hasOption("h")) {
            printHelp();
            System.exit(1);
        }
        log.info("Starting Openfeed Client with {}", config);
        OpenfeedClientExampleMain app = new OpenfeedClientExampleMain(config);
        app.start();
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("OpenfeedClient", options);
    }

    public OpenfeedClientExampleMain(OpenfeedClientConfigImpl config) {
        this.config = config;
    }

    public void start() throws CloneNotSupportedException, InterruptedException {
        config.setClientId("client");
        ConnectionStats connectionStats = new ConnectionStats();
        OpenfeedClientEventHandler eventHandler = new OpenfeedClientEventHandlerImpl(config,instrumentCache,connectionStats);
        OpenfeedClientHandler clientHandler = new OpenfeedClientHandlerImpl(config, instrumentCache,connectionStats);
        OpenfeedClientWebSocket client = new OpenfeedClientWebSocket(config, eventHandler, clientHandler);

        client.connectAndLogin();
    }

}

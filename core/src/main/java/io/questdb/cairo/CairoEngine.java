/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.MessageBusImpl;
import io.questdb.Metrics;
import io.questdb.cairo.mig.EngineMigration;
import io.questdb.cairo.pool.*;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.ReaderOutOfDateException;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cutlass.text.TextImportExecutionContext;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.WalTxnNotificationTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CairoEngine implements Closeable, WriterSource {
    public static final String BUSY_READER = "busyReader";
    private static final Log LOG = LogFactory.getLog(CairoEngine.class);
    private final AtomicLong asyncCommandCorrelationId = new AtomicLong();
    private final CairoConfiguration configuration;
    private final EngineMaintenanceJob engineMaintenanceJob;
    private final MessageBusImpl messageBus;
    private final MetadataPool metadataPool;
    private final Metrics metrics;
    private final ReaderPool readerPool;
    private final IDGenerator tableIdGenerator;
    private final TableNameRegistry tableNameRegistry;
    private final TableSequencerAPI tableSequencerAPI;
    private final MPSequence telemetryPubSeq;
    private final RingQueue<TelemetryTask> telemetryQueue;
    private final SCSequence telemetrySubSeq;
    private final TextImportExecutionContext textImportExecutionContext;
    // initial value of unpublishedWalTxnCount is 1 because we want to scan for unapplied WAL transactions on startup
    private final AtomicLong unpublishedWalTxnCount = new AtomicLong(1);
    private final WalWriterPool walWriterPool;
    private final WriterPool writerPool;


    // Kept for embedded API purposes. The second constructor (the one with metrics)
    // should be preferred for internal use.
    public CairoEngine(CairoConfiguration configuration) {
        this(configuration, Metrics.disabled());
    }

    public CairoEngine(CairoConfiguration configuration, Metrics metrics) {
        this.configuration = configuration;
        this.textImportExecutionContext = new TextImportExecutionContext(configuration);
        this.metrics = metrics;
        this.tableSequencerAPI = new TableSequencerAPI(this, configuration);
        this.messageBus = new MessageBusImpl(configuration);
        this.writerPool = new WriterPool(this.getConfiguration(), this.getMessageBus(), metrics);
        this.readerPool = new ReaderPool(configuration, this);
        this.metadataPool = new MetadataPool(configuration, this);
        this.walWriterPool = new WalWriterPool(configuration, this);
        this.engineMaintenanceJob = new EngineMaintenanceJob(configuration);
        if (configuration.getTelemetryConfiguration().getEnabled()) {
            this.telemetryQueue = new RingQueue<>(TelemetryTask::new, configuration.getTelemetryConfiguration().getQueueCapacity());
            this.telemetryPubSeq = new MPSequence(telemetryQueue.getCycle());
            this.telemetrySubSeq = new SCSequence();
            telemetryPubSeq.then(telemetrySubSeq).then(telemetryPubSeq);
        } else {
            this.telemetryQueue = null;
            this.telemetryPubSeq = null;
            this.telemetrySubSeq = null;
        }
        tableIdGenerator = new IDGenerator(configuration, TableUtils.TAB_INDEX_FILE_NAME);
        try {
            tableIdGenerator.open();
        } catch (Throwable e) {
            close();
            throw e;
        }
        // Recover snapshot, if necessary.
        try {
            DatabaseSnapshotAgent.recoverSnapshot(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
        // Migrate database files.
        try {
            EngineMigration.migrateEngineTo(this, ColumnType.VERSION, false);
        } catch (Throwable e) {
            close();
            throw e;
        }

        try {
            this.tableNameRegistry = configuration.isReadOnlyInstance() ?
                    new TableNameRegistryRO(configuration) : new TableNameRegistryRW(configuration);
            this.tableNameRegistry.reloadTableNameCache();
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @TestOnly
    public boolean clear() {
        boolean b1 = readerPool.releaseAll();
        boolean b2 = writerPool.releaseAll();
        boolean b3 = tableSequencerAPI.releaseAll();
        boolean b4 = metadataPool.releaseAll();
        boolean b5 = walWriterPool.releaseAll();
        messageBus.reset();
        return b1 & b2 & b3 & b4 & b5;
    }

    @Override
    public void close() {
        Misc.free(writerPool);
        Misc.free(readerPool);
        Misc.free(metadataPool);
        Misc.free(walWriterPool);
        Misc.free(tableIdGenerator);
        Misc.free(messageBus);
        Misc.free(tableSequencerAPI);
        Misc.free(telemetryQueue);
        Misc.free(tableNameRegistry);
    }

    @TestOnly
    public void closeNameRegistry() {
        tableNameRegistry.close();
    }

    public void createTable(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock
    ) {
        securityContext.checkWritePermission();
        CharSequence tableName = struct.getTableName();
        validNameOrThrow(tableName);

        int tableId = (int) tableIdGenerator.getNextId();
        TableToken systemTableName = registerTableName(tableName, tableId, struct.isWalEnabled());
        if (systemTableName == null) {
            if (ifNotExists) {
                return;
            }
            throw EntryUnavailableException.instance("table exists");
        }


        String tableNameStr = null;
        try {
            String lockedReason = lock(securityContext, systemTableName, "createTable");
            if (null == lockedReason) {
                boolean newTable = false;
                try {
                    int status = getStatus(securityContext, path, tableName);
                    if (status == TableUtils.TABLE_RESERVED) {
                        throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
                    }
                    tableNameStr = Chars.toString(tableName);
                    createTableUnsafe(
                            securityContext,
                            mem,
                            path,
                            struct,
                            systemTableName,
                            tableId
                    );
                    newTable = true;
                } finally {
                    if (!keepLock) {
                        readerPool.unlock(systemTableName);
                        writerPool.unlock(systemTableName, null, newTable);
                        metadataPool.unlock(systemTableName);
                        LOG.info().$("unlocked [systemTableName=`").utf8(systemTableName.getPrivateTableName()).$("`]").$();
                    }
                }
            } else {
                if (!ifNotExists) {
                    throw EntryUnavailableException.instance(lockedReason);
                }
            }
        } catch (Throwable th) {
            if (struct.isWalEnabled() && tableNameStr != null) {
                tableSequencerAPI.dropTable(tableNameStr, systemTableName, true);
            }
            throw th;
        }
    }

    // caller has to acquire the lock before this method is called and release the lock after the call
    public void createTableUnsafe(
            CairoSecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            TableStructure struct,
            TableToken systemTableName,
            int tableId
    ) {
        securityContext.checkWritePermission();

        // only create the table after it has been registered
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();
        final int mkDirMode = configuration.getMkDirMode();
        TableUtils.createTable(
                ff,
                root,
                mkDirMode,
                mem,
                path,
                systemTableName.getPrivateTableName(),
                struct,
                ColumnType.VERSION,
                tableId
        );
        if (struct.isWalEnabled()) {
            tableSequencerAPI.registerTable(tableId, struct, systemTableName);
        }
    }

    public void drop(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken systemTableName = getSystemTableName(tableName);

        if (isWalTableName(tableName)) {
            if (tableNameRegistry.removeWalTableName(tableName, systemTableName)) {
                tableSequencerAPI.dropTable(tableName, systemTableName, false);
            }
        } else {
            CharSequence lockedReason = lock(securityContext, systemTableName, "removeTable");
            if (null == lockedReason) {
                try {
                    path.of(configuration.getRoot()).concat(systemTableName.getPrivateTableName()).$();
                    int errno;
                    if ((errno = configuration.getFilesFacade().rmdir(path)) != 0) {
                        LOG.error().$("remove failed [tableName='").utf8(tableName).$("', error=").$(errno).$(']').$();
                        throw CairoException.critical(errno).put("could not remove table [name=").put(tableName)
                                .put(", systemTableName=").put(systemTableName.getPrivateTableName()).put(']');
                    }
                } finally {
                    unlock(securityContext, tableName, null, false);
                }

                tableNameRegistry.removeTableName(tableName, systemTableName);
                return;
            }
            throw CairoException.nonCritical().put("Could not lock '").put(tableName).put("' [reason='").put(lockedReason).put("']");
        }
    }

    public TableWriter getBackupWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence backupDirName
    ) {
        securityContext.checkWritePermission();
        // There is no point in pooling/caching these writers since they are only used once, backups are not incremental
        TableToken systemTableName = getSystemTableName(tableName);
        return new TableWriter(
                configuration,
                systemTableName,
                messageBus,
                null,
                true,
                DefaultLifecycleManager.INSTANCE,
                backupDirName,
                Metrics.disabled()
        );
    }

    @TestOnly
    public int getBusyReaderCount() {
        return readerPool.getBusyCount();
    }

    @TestOnly
    public int getBusyWriterCount() {
        return writerPool.getBusyCount();
    }

    public long getCommandCorrelationId() {
        return asyncCommandCorrelationId.incrementAndGet();
    }

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public Job getEngineMaintenanceJob() {
        return engineMaintenanceJob;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, TableToken systemTableName) {
        try {
            return metadataPool.get(systemTableName);
        } catch (CairoException e) {
            tryRepairTable(securityContext, systemTableName, e);
        }
        return metadataPool.get(systemTableName);
    }

    public TableRecordMetadata getMetadata(CairoSecurityContext securityContext, TableToken systemTableName, long structureVersion) {
        try {
            final TableRecordMetadata metadata = metadataPool.get(systemTableName);
            if (structureVersion != TableUtils.ANY_TABLE_VERSION && metadata.getStructureVersion() != structureVersion) {
                // rename to StructureVersionException?
                final ReaderOutOfDateException ex = ReaderOutOfDateException.of(systemTableName.getPublicTableName(), metadata.getTableId(), metadata.getTableId(), structureVersion, metadata.getStructureVersion());
                metadata.close();
                throw ex;
            }
            return metadata;
        } catch (CairoException e) {
            tryRepairTable(securityContext, systemTableName, e);
        }
        return metadataPool.get(systemTableName);
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public TableToken getOrCreateSystemTableName(final CharSequence tableName, boolean isWal) {
        TableToken systemTableName = tableNameRegistry.getSystemName(tableName);
        if (systemTableName == null) {
            systemTableName = registerTableName(tableName, isWal);
            return systemTableName != null ? systemTableName : tableNameRegistry.getSystemName(tableName);
        }
        return systemTableName;
    }

    @TestOnly
    public PoolListener getPoolListener() {
        return this.writerPool.getPoolListener();
    }

    public TableReader getReader(CairoSecurityContext securityContext, CharSequence tableName) {
        return getReader(securityContext, tableName, TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
    }

    public TableReader getReader(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            int tableId,
            long version
    ) {
        validNameOrThrow(tableName);
        TableToken systemTableName = getSystemTableName(tableName);
        TableReader reader = readerPool.get(systemTableName);
        if ((version > -1 && reader.getVersion() != version)
                || tableId > -1 && reader.getMetadata().getTableId() != tableId) {
            ReaderOutOfDateException ex = ReaderOutOfDateException.of(tableName, tableId, reader.getMetadata().getTableId(), version, reader.getVersion());
            reader.close();
            throw ex;
        }
        return reader;
    }

    public TableReader getReaderBySystemName(@SuppressWarnings("unused") CairoSecurityContext securityContext, TableToken systemTableName) {
        return readerPool.get(systemTableName);
    }

    public Map<TableToken, AbstractMultiTenantPool.Entry<ReaderPool.R>> getReaderPoolEntries() {
        return readerPool.entries();
    }

    public TableReader getReaderWithRepair(CairoSecurityContext securityContext, CharSequence tableName) {

        validNameOrThrow(tableName);

        try {
            return getReader(securityContext, tableName);
        } catch (CairoException e) {
            // Cannot open reader on existing table is pretty bad.
            // In some messed states, for example after _meta file swap failure Reader cannot be opened
            // but writer can be. Opening writer fixes the table mess.
            TableToken systemTableName = getSystemTableName(tableName);
            tryRepairTable(securityContext, systemTableName, e);
        }
        try {
            return getReader(securityContext, tableName);
        } catch (CairoException e) {
            LOG.critical()
                    .$("could not open reader [table=").$(tableName)
                    .$(", errno=").$(e.getErrno())
                    .$(", error=").$(e.getMessage()).I$();
            throw e;
        }
    }

    public int getStatus(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            int lo,
            int hi
    ) {
        StringSink sink = Misc.getThreadLocalBuilder();
        sink.put(tableName, lo, hi);
        TableToken systemTableName = tableNameRegistry.getSystemName(sink);
        if (systemTableName == null) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), systemTableName.getPrivateTableName());
    }

    public int getStatus(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName
    ) {
        TableToken systemTableName = tableNameRegistry.getSystemName(tableName);
        if (systemTableName == null) {
            return TableUtils.TABLE_DOES_NOT_EXIST;
        }
        return TableUtils.exists(configuration.getFilesFacade(), path, configuration.getRoot(), systemTableName.getPrivateTableName());
    }

    public TableToken getSystemTableName(final CharSequence tableName) {
        TableToken systemTableName = tableNameRegistry.getSystemName(tableName);
        if (systemTableName == null) {
            throw CairoException.nonCritical().put("table does not exist [table=").put(tableName).put("]");
        }
        return systemTableName;
    }

    public IDGenerator getTableIdGenerator() {
        return tableIdGenerator;
    }

    public String getTableNameBySystemName(TableToken systemTableName) {
        String tableName = tableNameRegistry.getTableNameBySystemName(systemTableName);
        if (tableName == null && !tableNameRegistry.isWalTableDropped(systemTableName)) {
            throw CairoException.nonCritical().put("table does not exist [privateTableName=").put(systemTableName.getPrivateTableName()).put("]");
        }
        return tableName;
    }

    public TableSequencerAPI getTableSequencerAPI() {
        return tableSequencerAPI;
    }

    public Iterable<TableToken> getTableSystemNames() {
        return tableNameRegistry.getTableSystemNames();
    }

    @Override
    public TableWriterAPI getTableWriterAPI(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable String lockReason
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken tableNameRecord = tableNameRegistry.getSystemName(tableName);
        if (tableNameRecord == null) {
            throw CairoException.nonCritical().put("table does not exist [table=").put(tableName).put("]");
        }

        if (!tableNameRecord.isWal()) {
            return writerPool.get(tableNameRecord, lockReason);

        }
        return walWriterPool.get(tableNameRecord);
    }

    public Sequence getTelemetryPubSequence() {
        return telemetryPubSeq;
    }

    public RingQueue<TelemetryTask> getTelemetryQueue() {
        return telemetryQueue;
    }

    public SCSequence getTelemetrySubSequence() {
        return telemetrySubSeq;
    }

    public TextImportExecutionContext getTextImportExecutionContext() {
        return textImportExecutionContext;
    }

    public long getUnpublishedWalTxnCount() {
        return unpublishedWalTxnCount.get();
    }

    // For testing only
    @TestOnly
    public WalReader getWalReader(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            CharSequence tableName,
            CharSequence walName,
            int segmentId,
            long walRowCount
    ) {
        TableToken tableNameRecord = tableNameRegistry.getSystemName(tableName);
        if (tableNameRecord != null && tableNameRecord.isWal()) {
            // This is WAL table because sequencer exists
            return new WalReader(configuration, tableNameRecord, walName, segmentId, walRowCount);
        }

        throw CairoException.nonCritical().put("WAL reader is not supported for table ").put(tableName);
    }

    @TestOnly
    public @NotNull WalWriter getWalWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        return walWriterPool.get(getSystemTableName(tableName));
    }

    public TableWriter getWriter(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken systemTableName = getSystemTableName(tableName);
        return writerPool.get(systemTableName, lockReason);
    }

    public TableWriter getWriterBySystemName(
            CairoSecurityContext securityContext,
            TableToken systemTableName,
            String lockReason
    ) {
        securityContext.checkWritePermission();
        return writerPool.get(systemTableName, lockReason);
    }

    public TableWriter getWriterOrPublishCommand(
            CairoSecurityContext securityContext,
            CharSequence tableName,
            @NotNull AsyncWriterCommand asyncWriterCommand
    ) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        TableToken systemTableName = getSystemTableName(tableName);
        return writerPool.getWriterOrPublishCommand(systemTableName, asyncWriterCommand.getCommandName(), asyncWriterCommand);
    }

    public boolean isWalTableDropped(TableToken systemTableName) {
        return tableNameRegistry.isWalTableDropped(systemTableName);
    }

    public boolean isWalTableName(CharSequence tableName) {
        return tableNameRegistry.isWalTableName(tableName);
    }

    public String lock(
            CairoSecurityContext securityContext,
            TableToken systemTableName,
            String lockReason
    ) {
        assert null != lockReason;
        securityContext.checkWritePermission();

        String lockedReason = writerPool.lock(systemTableName, lockReason);
        if (lockedReason == null) { // not locked
            if (readerPool.lock(systemTableName)) {
                if (metadataPool.lock(systemTableName)) {
                    LOG.info().$("locked [table=`").utf8(systemTableName.getPrivateTableName()).$("`, thread=").$(Thread.currentThread().getId()).I$();
                    return null;
                }
                readerPool.unlock(systemTableName);
            }
            writerPool.unlock(systemTableName);
            return BUSY_READER;
        }
        return lockedReason;

    }

    public boolean lockReaders(CharSequence tableName) {
        validNameOrThrow(tableName);
        return readerPool.lock(getSystemTableName(tableName));
    }

    public boolean lockReadersBySystemName(TableToken systemTableName) {
        return readerPool.lock(systemTableName);
    }

    public CharSequence lockWriter(CairoSecurityContext securityContext, CharSequence tableName, String lockReason) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        return writerPool.lock(getSystemTableName(tableName), lockReason);
    }

    public void notifyWalTxnCommitted(int tableId, TableToken systemTableName, long txn) {
        final Sequence pubSeq = messageBus.getWalTxnNotificationPubSequence();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                WalTxnNotificationTask task = messageBus.getWalTxnNotificationQueue().get(cursor);
                task.of(systemTableName, tableId, txn);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                LOG.info().$("cannot publish WAL notifications, queue is full [current=")
                        .$(pubSeq.current()).$(", table=").utf8(systemTableName.getPrivateTableName())
                        .I$();
                // queue overflow, throw away notification and notify a job to rescan all tables
                notifyWalTxnRepublisher();
                return;
            }
        }
    }

    public void notifyWalTxnRepublisher() {
        unpublishedWalTxnCount.incrementAndGet();
    }

    public TableToken registerTableName(CharSequence tableName, boolean isWal) {
        int tableId = (int) getTableIdGenerator().getNextId();
        return registerTableName(tableName, tableId, isWal);
    }

    @Nullable
    public TableToken registerTableName(CharSequence tableName, int tableId, boolean isWal) {
        String tableNameStr = Chars.toString(tableName);
        String systemTableName = tableNameStr;
        if (isWal) {
            systemTableName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
            systemTableName += tableId;
        } else if (configuration.mangleTableSystemNames()) {
            systemTableName += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        }
        return tableNameRegistry.registerName(tableNameStr, systemTableName, tableId, isWal);
    }

    @TestOnly
    public boolean releaseAllReaders() {
        boolean b1 = metadataPool.releaseAll();
        return readerPool.releaseAll() & b1;
    }

    @TestOnly
    public void releaseAllWriters() {
        writerPool.releaseAll();
    }

    public boolean releaseInactive() {
        boolean useful = writerPool.releaseInactive();
        useful |= readerPool.releaseInactive();
        useful |= tableSequencerAPI.releaseInactive();
        useful |= metadataPool.releaseInactive();
        useful |= walWriterPool.releaseInactive();
        return useful;
    }

    @TestOnly
    public void releaseInactiveTableSequencers() {
        walWriterPool.releaseInactive();
        tableSequencerAPI.releaseInactive();
    }

    public void releaseReadersBySystemName(TableToken systemTableName) {
        readerPool.unlock(systemTableName);
    }

    @TestOnly
    public void reloadTableNames() {
        tableNameRegistry.reloadTableNameCache();
    }

    public int removeDirectory(@Transient Path path, CharSequence dir) {
        path.of(configuration.getRoot()).concat(dir);
        final FilesFacade ff = configuration.getFilesFacade();
        return ff.rmdir(path.slash$());
    }

    public void removeTableSystemName(TableToken tableName) {
        tableNameRegistry.removeTableSystemName(tableName);
    }

    public void rename(
            CairoSecurityContext securityContext,
            Path path,
            CharSequence tableName,
            Path otherPath,
            CharSequence newName
    ) {
        securityContext.checkWritePermission();

        validNameOrThrow(tableName);
        validNameOrThrow(newName);

        TableToken tableNameRecord = getSystemTableName(tableName);
        if (tableNameRecord != null) {
            if (tableNameRecord.isWal()) {
                String newTableNameStr = tableNameRegistry.rename(tableName, newName, tableNameRecord);
                tableSequencerAPI.renameWalTable(tableName, newTableNameStr, tableNameRecord);
            } else {
                String lockedReason = lock(securityContext, tableNameRecord, "renameTable");
                if (null == lockedReason) {
                    try {
                        rename0(path, tableNameRecord, tableName, otherPath, newName);
                    } finally {
                        unlock(securityContext, tableName, null, false);
                    }
                    tableNameRegistry.removeTableName(tableName, tableNameRecord);
                } else {
                    LOG.error().$("cannot lock and rename [from='").$(tableName).$("', to='").$(newName).$("', reason='").$(lockedReason).$("']").$();
                    throw EntryUnavailableException.instance(lockedReason);
                }
            }
        } else {
            LOG.error().$('\'').utf8(tableName).$("' does not exist. Rename failed.").$();
            throw CairoException.nonCritical().put("Rename failed. Table '").put(tableName).put("' does not exist");
        }
    }

    @TestOnly
    public void resetNameRegistryMemory() {
        tableNameRegistry.resetMemory();
    }

    @TestOnly
    public void setPoolListener(PoolListener poolListener) {
        this.metadataPool.setPoolListener(poolListener);
        this.writerPool.setPoolListener(poolListener);
        this.readerPool.setPoolListener(poolListener);
        this.walWriterPool.setPoolListener(poolListener);
    }

    public void unlock(
            @SuppressWarnings("unused") CairoSecurityContext securityContext,
            CharSequence tableName,
            @Nullable TableWriter writer,
            boolean newTable
    ) {
        validNameOrThrow(tableName);
        TableToken systemTableName = getSystemTableName(tableName);
        readerPool.unlock(systemTableName);
        writerPool.unlock(systemTableName, writer, newTable);
        metadataPool.unlock(systemTableName);
        LOG.info().$("unlocked [table=`").utf8(tableName).$("`]").$();
    }

    public void unlockReaders(CharSequence tableName) {
        validNameOrThrow(tableName);
        readerPool.unlock(getSystemTableName(tableName));
    }

    public void unlockWriter(CairoSecurityContext securityContext, CharSequence tableName) {
        securityContext.checkWritePermission();
        validNameOrThrow(tableName);
        writerPool.unlock(getSystemTableName(tableName));
    }

    private void rename0(Path path, TableToken systemTableName, CharSequence tableName, Path otherPath, CharSequence to) {
        final FilesFacade ff = configuration.getFilesFacade();
        final CharSequence root = configuration.getRoot();

        path.of(root).concat(systemTableName.getPrivateTableName()).$();

        TableToken dstFileName = registerTableName(to, false);
        try {
            otherPath.of(root).concat(dstFileName == null ? "" : dstFileName.getPrivateTableName()).$();
            if (dstFileName == null || ff.exists(otherPath)) {
                LOG.error().$("rename target exists [from='").$(tableName).$("', to='").utf8(otherPath).I$();
                throw CairoException.nonCritical().put("Rename target exists");
            }

            if (ff.rename(path, otherPath) != Files.FILES_RENAME_OK) {
                int error = ff.errno();
                LOG.error().$("could not rename [from='").$(path).$("', to='").utf8(otherPath).$("', error=").$(error).I$();
                throw CairoException.critical(error)
                        .put("could not rename [from='").put(path)
                        .put("', to='").put(otherPath)
                        .put("', error=").put(error);
            }
        } catch (Throwable ex) {
            if (dstFileName != null) {
                // release the destination name
                tableNameRegistry.removeTableName(to, systemTableName);
            }
            throw ex;
        }
    }

    private void tryRepairTable(
            CairoSecurityContext securityContext,
            TableToken systemTableName,
            RuntimeException rethrow
    ) {
        try {
            securityContext.checkWritePermission();
            writerPool.get(systemTableName, "repair").close();
        } catch (EntryUnavailableException e) {
            // This is fine, writer is busy. Throw back origin error.
            throw rethrow;
        } catch (Throwable th) {
            LOG.critical()
                    .$("could not repair before reading [systemTableName=").utf8(systemTableName.getPrivateTableName())
                    .$(" ,error=").$(th.getMessage()).I$();
            throw rethrow;
        }
    }

    private void validNameOrThrow(CharSequence tableName) {
        if (!TableUtils.isValidTableName(tableName, configuration.getMaxFileNameLength())) {
            throw CairoException.nonCritical()
                    .put("invalid table name [table=").putAsPrintable(tableName)
                    .put(']');
        }
    }

    private class EngineMaintenanceJob extends SynchronizedJob {

        private final long checkInterval;
        private final MicrosecondClock clock;
        private long last = 0;

        public EngineMaintenanceJob(CairoConfiguration configuration) {
            this.clock = configuration.getMicrosecondClock();
            this.checkInterval = configuration.getIdleCheckInterval() * 1000;
        }

        @Override
        protected boolean runSerially() {
            long t = clock.getTicks();
            if (last + checkInterval < t) {
                last = t;
                return releaseInactive();
            }
            return false;
        }
    }
}

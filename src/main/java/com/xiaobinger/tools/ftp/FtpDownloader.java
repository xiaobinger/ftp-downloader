package com.xiaobinger.tools.ftp;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.extra.ftp.AbstractFtp;
import cn.hutool.extra.ftp.Ftp;
import cn.hutool.extra.ftp.FtpConfig;
import cn.hutool.extra.ftp.FtpMode;
import cn.hutool.extra.ssh.Sftp;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;

/**
 * @author xiongbing
 * @date 2024/11/20 13:38
 * @description FTP工具类，用于远程文件下载
 */
public class FtpDownloader {
    private static final Logger log = LoggerFactory.getLogger(FtpDownloader.class);
    private final boolean isSftp;
    public AbstractFtp ftp;
    private final FtpConfig ftpConfig;
    private static final int DEFAULT_THREAD_COUNT = 5;
    private static final long DEFAULT_CONNECTION_TIMEOUT = 30000L;
    private static final long DEFAULT_SO_TIMEOUT = 30000L;


    /**
     * 构造FTP下载器
     * @param ftpConfig FTP配置
     * @param isSftp 是否使用SFTP
     */
    public FtpDownloader(FtpConfig ftpConfig, boolean isSftp) {
        this.ftpConfig = ftpConfig;
        this.isSftp = isSftp;
    }

    /**
     * 构造FTP下载器
     * 
     * @param host FTP服务器地址
     * @param port FTP服务器端口
     * @param username 用户名
     * @param password 密码
     */
    public FtpDownloader(String host, int port, String username, String password, boolean isSftp) {
        this(host, port, username, password, StandardCharsets.UTF_8,isSftp);
    }
    
    /**
     * 构造FTP下载器
     * 
     * @param host FTP服务器地址
     * @param port FTP服务器端口
     * @param username 用户名
     * @param password 密码
     * @param charset 字符集
     */
    public FtpDownloader(String host, int port, String username, String password, Charset charset, boolean isSftp) {
        this.ftpConfig = FtpConfig.create()
                .setHost(host)
                .setPort(port)
                .setUser(username)
                .setPassword(password)
                .setCharset(charset)
                // 30秒超时
                .setSoTimeout(DEFAULT_SO_TIMEOUT)
                .setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
        this.isSftp = isSftp;
    }

    /**
     * 连接到FTP服务器
     */
    public synchronized void connect() {
        // 如果已经连接且连接有效，则直接返回
        if (ftp != null) {
            ftp = ftp.reconnectIfTimeout();
            return;
        }
        try {
            if (isSftp) {
                ftp = new Sftp(ftpConfig);
            } else {
                ftp = new Ftp(ftpConfig, FtpMode.Passive);
            }
            log.info("成功连接到FTP服务器: {}:{}", ftpConfig.getHost(), ftpConfig.getPort());
        } catch (Exception e) {
            log.error("连接FTP服务器失败: {}:{}", ftpConfig.getHost(), ftpConfig.getPort(), e);
            throw new RuntimeException("FTP连接失败", e);
        }
    }
    
    /**
     * 断开FTP连接
     */
    public synchronized void disconnect() {
        if (ftp != null) {
            try {
                ftp.close();
                log.info("已断开FTP连接");
            } catch (Exception e) {
                log.warn("断开FTP连接时发生异常", e);
            } finally {
                ftp = null;
            }
        }
    }
    
    /**
     * 下载文件
     * 
     * @param remotePath 远程文件路径
     * @param localFile 本地文件
     * @return 是否下载成功
     */
    public boolean downloadFile(String remotePath, File localFile) {
        return downloadFile(remotePath, "", localFile);
    }
    
    /**
     * 下载文件
     * 
     * @param remotePath 远程目录路径
     * @param remoteFileName 远程文件名
     * @param localFile 本地文件
     * @return 是否下载成功
     */
    public boolean downloadFile(String remotePath, String remoteFileName, File localFile) {
        try {
            connect();
            if (!fileExists(remotePath, remoteFileName)) {
                log.error("文件不存在: {}/{}", remotePath, remoteFileName);
                disconnect();
                return false;
            }
            String fullPath = remotePath;
            if (StrUtil.isNotBlank(remoteFileName)) {
                fullPath = StrUtil.addSuffixIfNot(remotePath, "/") + remoteFileName;
            }
            log.info("开始下载文件: {} 到本地: {}", fullPath, localFile.getAbsolutePath());
            ftp.download(fullPath, localFile);
            boolean success = FileUtil.exist(localFile);
            if (success) {
                log.info("文件下载成功: {} (大小: {} 字节)", localFile.getAbsolutePath(), localFile.length());
            } else {
                log.error("文件下载失败: {}", fullPath);
                // 如果下载失败，删除可能创建的空文件
                if (localFile.exists()) {
                    FileUtil.del(localFile);
                }
                disconnect();
            }
            return success;
        } catch (Exception e) {
            log.error("下载文件时发生异常: {}/{}", remotePath, remoteFileName, e);
            disconnect();
            return false;
        }
    }
    
    /**
     * 下载文件到指定路径
     * 
     * @param remotePath 远程目录路径
     * @param remoteFileName 远程文件名
     * @param localPath 本地文件路径
     * @return 是否下载成功
     */
    public boolean downloadFile(String remotePath, String remoteFileName, String localPath) {
        File localFile = new File(localPath);
        // 确保父目录存在
        FileUtil.mkParentDirs(localFile);
        return downloadFile(remotePath, remoteFileName, localFile);
    }
    
    /**
     * 检查文件是否存在
     * 
     * @param remotePath 远程目录路径
     * @param remoteFileName 远程文件名
     * @return 文件是否存在
     */
    public boolean fileExists(String remotePath, String remoteFileName) {
        try {
            connect();
            return ftp.exist(remotePath+"/"+remoteFileName);
        } catch (Exception e) {
            log.error("检查文件是否存在时发生异常: {}/{}", remotePath, remoteFileName, e);
            return false;
        }
    }
    
    /**
     * 列出目录下的文件
     * 
     * @param remotePath 远程目录路径
     * @return 文件列表
     */
    public String[] listFiles(String remotePath) {
        try {
            connect();
            return ftp.ls(remotePath).toArray(new String[0]);
        } catch (Exception e) {
            log.error("列出目录文件时发生异常: {}", remotePath, e);
            return new String[0];
        }
    }

    /**
     * 多线程下载文件夹下的多个文件
     *
     * @param remoteDir     远程目录路径
     * @param localDir      本地目录路径
     * @param threadCount   下载线程数
     * @return 下载结果
     */
    public DownloadResult downloadDirectoryMultithread(String remoteDir, String localDir, int threadCount) {
        return downloadDirectoryMultithread(remoteDir, localDir, threadCount, false, null,null);
    }

    /**
     * 多线程下载文件夹下的多个文件
     *
     * @param remoteDir   远程目录路径
     * @param localDir    本地目录路径
     * @param threadCount 下载线程数
     * @param zipResult   是否将下载的文件打包成ZIP
     * @param zipFileName ZIP文件名（如果不指定则使用默认名称）
     * @return 下载结果
     */
    public DownloadResult downloadDirectoryMultithread(String remoteDir, String localDir, int threadCount,
                                                       boolean zipResult, String zipFileName) {
        return downloadDirectoryMultithread(remoteDir, localDir, threadCount, zipResult, zipFileName, null);
    }

    /**
     * 多线程下载文件夹下的多个文件（支持文件过滤）
     *
     * @param remoteDir   远程目录路径
     * @param localDir    本地目录路径
     * @param threadCount 下载线程数
     * @param zipResult   是否将下载的文件打包成ZIP
     * @param zipFileName ZIP文件名（如果不指定则使用默认名称）
     * @param filter      文件过滤器，用于筛选需要下载的文件
     * @return 下载结果
     */
    public DownloadResult downloadDirectoryMultithread(String remoteDir, String localDir, int threadCount,
                                                       boolean zipResult, String zipFileName,
                                                       Predicate<String> filter) {
        long startTime = System.currentTimeMillis();
        DownloadResult result = new DownloadResult();

        // 参数校验
        if (StrUtil.isBlank(remoteDir)) {
            result.setSuccess(false);
            result.setErrorMessage("远程目录路径不能为空");
            return result;
        }

        if (StrUtil.isBlank(localDir)) {
            result.setSuccess(false);
            result.setErrorMessage("本地目录路径不能为空");
            return result;
        }

        if (threadCount <= 0) {
            threadCount = DEFAULT_THREAD_COUNT;
        }

        ExecutorService executor = null;
        try {
            // 创建本地目录
            File localDirFile = new File(localDir);
            if (!localDirFile.exists()) {
                FileUtil.mkdir(localDirFile);
            }

            // 获取远程目录下的所有文件
            String[] files = listFiles(remoteDir);
            if (files.length == 0) {
                log.warn("远程目录 {} 中没有文件", remoteDir);
                result.setSuccess(true);
                return result;
            }

            // 应用文件过滤器
            if (filter != null) {
                files = java.util.Arrays.stream(files)
                        .filter(filter)
                        .toArray(String[]::new);
                log.info("文件过滤后剩余 {} 个文件", files.length);
            }

            if (files.length == 0) {
                log.warn("文件过滤后没有符合条件的文件");
                result.setSuccess(true);
                return result;
            }

            log.info("开始多线程下载 {} 个文件，线程数: {}", files.length, threadCount);

            // 创建线程池
            executor = Executors.newFixedThreadPool(threadCount);
            CompletionService<DownloadTaskResult> completionService =
                    new ExecutorCompletionService<>(executor);

            // 提交下载任务
            int taskCount = 0;
            for (String fileName : files) {
                // 跳过目录
                if (isDirectory(remoteDir, fileName)) {
                    log.debug("跳过目录: {}", fileName);
                    continue;
                }

                DownloadTask task = new DownloadTask(remoteDir, fileName, localDir);
                completionService.submit(task);
                taskCount++;
            }

            if (taskCount == 0) {
                log.warn("没有需要下载的文件");
                result.setSuccess(true);
                return result;
            }

            // 收集下载结果
            int successCount = 0;
            for (int i = 0; i < taskCount; i++) {
                try {
                    DownloadTaskResult taskResult = completionService.take().get(30, TimeUnit.SECONDS);
                    if (taskResult.isSuccess()) {
                        successCount++;
                        result.addSuccessfulFile(taskResult.getFileName());
                    } else {
                        result.addFailedFile(taskResult.getFileName(), taskResult.getErrorMessage());
                    }
                } catch (TimeoutException e) {
                    log.error("下载任务超时", e);
                    result.addFailedFile("unknown", "下载任务超时");
                } catch (ExecutionException e) {
                    log.error("下载任务执行异常", e);
                    result.addFailedFile("unknown", e.getMessage());
                } catch (InterruptedException e) {
                    log.error("下载任务被中断", e);
                    Thread.currentThread().interrupt();
                    result.addFailedFile("unknown", "下载任务被中断");
                }
            }

            result.setSuccess(successCount == taskCount);
            result.setTotalFiles(taskCount);
            result.setSuccessfulFiles(successCount);
            result.setFailedFiles(taskCount - successCount);

            log.info("多线程下载完成，总计: {}，成功: {}，失败: {}，耗时: {}ms",
                    taskCount, successCount, taskCount - successCount,
                    System.currentTimeMillis() - startTime);

            // 如果需要打包成ZIP
            if (result.isSuccess() && zipResult) {
                String zipPath = createZipFromDirectory(localDir, zipFileName);
                result.setZipFilePath(zipPath);
                log.info("文件已打包成ZIP: {}", zipPath);
                // 删除原始下载的文件目录
                try {
                    FileUtil.del(localDir);
                    log.info("已删除原始下载目录: {}", localDir);
                } catch (Exception e) {
                    log.warn("删除原始下载目录时发生异常: {}", localDir, e);
                }
            }
        } catch (Exception e) {
            log.error("多线程下载文件夹时发生异常", e);
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        } finally {
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            disconnect();
        }

        return result;
    }

    /**
     * 判断是否为目录
     */
    private boolean isDirectory(String remoteDir, String fileName) {
        try {
            connect();
            String fullPath = StrUtil.addSuffixIfNot(remoteDir, "/") + fileName;
            return ftp.isDir(fullPath);
        } catch (Exception e) {
            log.warn("判断文件 {} 是否为目录时发生异常", fileName, e);
            return false;
        }
    }

    /**
     * 将目录打包成ZIP文件
     */
    private String createZipFromDirectory(String directoryPath, String zipFileName) {
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("指定路径不是有效目录: " + directoryPath);
        }

        String zipPath;
        if (StrUtil.isNotBlank(zipFileName)) {
            zipPath = dir.getParent() + File.separator + zipFileName;
        } else {
            zipPath = directoryPath + ".zip";
        }

        ZipUtil.zip(directoryPath,zipPath);
        return zipPath;
    }

    /**
     * 下载任务类
     */
    private class DownloadTask implements Callable<DownloadTaskResult> {
        private final String remoteDir;
        private final String fileName;
        private final String localDir;

        public DownloadTask(String remoteDir, String fileName, String localDir) {
            this.remoteDir = remoteDir;
            this.fileName = fileName;
            this.localDir = localDir;
        }

        @Override
        public DownloadTaskResult call() {
            DownloadTaskResult result = new DownloadTaskResult();
            result.setFileName(fileName);
            FtpDownloader downloader;
            try {
                downloader = new FtpDownloader(
                        ftpConfig.getHost(),
                        ftpConfig.getPort(),
                        ftpConfig.getUser(),
                        ftpConfig.getPassword(),
                        ftpConfig.getCharset(),
                        isSftp);
                boolean success = downloader.downloadFile(remoteDir, fileName, localDir + File.separator + fileName);
                result.setSuccess(success);
                if (!success) {
                    result.setErrorMessage("下载失败");
                }
            } catch (Exception e) {
                result.setSuccess(false);
                result.setErrorMessage(e.getMessage());
                log.error("下载文件 {} 时发生异常", fileName, e);
            }
            return result;
        }
    }

    /**
     * 下载结果类
     */
    @Data
    public static class DownloadResult {
        private boolean success;
        private int totalFiles;
        private int successfulFiles;
        private int failedFiles;
        private String errorMessage;
        private String zipFilePath;
        private final List<String> successfulFilesList = new java.util.ArrayList<>();
        private final java.util.Map<String, String> failedFilesMap = new java.util.HashMap<>();
        public void addSuccessfulFile(String fileName) {
            successfulFilesList.add(fileName);
        }
        public void addFailedFile(String fileName, String error) {
            failedFilesMap.put(fileName, error);
        }
    }

    /**
     * 下载任务结果类
     */
    @Data
    private static class DownloadTaskResult {
        private String fileName;
        private boolean success;
        private String errorMessage;
    }
}

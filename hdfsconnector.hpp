/*##############################################################################

 Copyright (C) 2012 HPCC Systems.

 All rights reserved. This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 ############################################################################## */

#include <stdio.h>
#include <string.h>
#include <sstream>
#include <stdlib.h>
#include <curl/curl.h>

using namespace std;

#define EOL "\n"

#define WEBHDFS_VER_PATH "/webhdfs/v1"

const static int RETURN_FAILURE = -1;

struct WebHdfsFileStatus
{
   long accessTime;
   int blockSize;
   const char * group;
   long length;
   long modificationTime;
   const char * owner;
   const char * pathSuffix;
   const char * permission;
   int replication;
   const char * type; //"FILE" | "DIRECTORY"
};

template <class T>
inline std::string template2string (const T& anything)
{
    std::stringstream ss;
    ss << anything;
    return ss.str();
}

static size_t readStringCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode = 0;
    curl_off_t nread;

    if (stream && ((string *)stream)->size() > 0)
    {
        retcode = sprintf((char*)ptr, "%s", ((string *)stream)->c_str());
        ((string *)stream)->clear();
    }

    return retcode;
}

static size_t readBufferCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode = 0;
    curl_off_t nread;

    if (stream && strlen((char *)stream) > 0)
    {
        retcode = sprintf((char*)ptr, "%s", (char *)stream);
        ((char *)stream)[0] = '\0';
    }

    return retcode;
}

static size_t continueCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    return 0;
}

static size_t readFileCallBackCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    size_t retcode;
    curl_off_t nread;

    retcode = fread(ptr, size, nmemb, (_IO_FILE *)stream);

  return retcode;
}

static size_t writeToBufferCurl(void *ptr, size_t size, size_t nmemb, void *stream)
{
    if (stream)
        sprintf((char *)stream, "%s", (char*)ptr);

    return size*nmemb;
}

static size_t writeToStrCallBackCurl( void *ptr, size_t size, size_t nmemb, void *stream)
{
    if (stream)
    {
        ((std::string *)stream)->append((char*)ptr, size*nmemb);
    }
    return size*nmemb;
}

static size_t writeToStdOutCallBackCurl( void *ptr, size_t size, size_t nmemb, void *stream)
{
    fprintf(stdout, "%s", (char*)ptr);
    return size*nmemb;
}

static size_t writeToStdErrCallBackCurl( void *ptr, size_t size, size_t nmemb, void *stream)
{
    fprintf(stderr, "%s", (char*)ptr);
    return size*nmemb;
}

static long getRecordCount(long fsize, int clustersize, int reclen, int nodeid)
{
    long readSize = fsize / reclen / clustersize;
    if (fsize % reclen)
    {
        fprintf(stderr, "filesize (%lu) not multiple of record length(%d)", fsize, reclen);
        return RETURN_FAILURE;
    }

    if ((fsize / reclen) % clustersize > nodeid)
    {
        readSize++;
        fprintf(stderr, "\nThis node will pipe one extra rec\n");
    }
    return readSize;
}

static inline void createFilePartName(string * filepartname, const char * filename, unsigned int nodeid, unsigned int clustercount)
{
    filepartname->append(filename);
    filepartname->append("-parts/part_");
    filepartname->append(template2string(nodeid));
    filepartname->append("_");
    filepartname->append(template2string(clustercount));
}

class WebHdfs_Connector
{
private:
    string baseurl;
    string targetfileurl;
    string username;
    bool hasusername;
    bool webhdfsreached;
    CURL *curl;

    WebHdfsFileStatus targetfilestatus;

public:

    WebHdfs_Connector(const char * hadoopHost, const char * hadoopPort, const char * hadoopUser, const char * fileName)
    {
        curl_global_init(CURL_GLOBAL_DEFAULT);
        curl = curl_easy_init();

        if (!curl)
        {
            fprintf(stderr, "Could not connect to WebHDFS\n");
            return;
        }

        baseurl.clear();
        targetfileurl.clear();
        hasusername = false;

        baseurl.append(hadoopHost);
        baseurl.append(":");
        baseurl.append(hadoopPort);
        baseurl.append(WEBHDFS_VER_PATH);

        targetfileurl.assign(baseurl.c_str());
        baseurl.append("/");
        if (fileName[0] != '/')
            targetfileurl.append("/");

        targetfileurl.append(fileName);

        if (strlen(hadoopUser)>0)
        {
            username.assign(hadoopUser);
            hasusername = true;
        }

        webhdfsreached = (reachWebHDFS() == EXIT_SUCCESS);

        if (webhdfsreached)
            getFileStatusWeb(targetfileurl.c_str(), &targetfilestatus);
    };

    ~WebHdfs_Connector()
    {
        curl_easy_cleanup(curl);
    };

    int reachWebHDFS();
    int readFileOffsetWebToSTDOut(unsigned long seekPos, unsigned long readlen, int maxretries);
    int readCSVOffsetWeb(unsigned long seekPos,
            unsigned long readlen, const char * eolseq, unsigned long bufferSize, bool outputTerminator,
            unsigned long recLen, unsigned long maxLen, const char * quote, int maxretries);
    int writeFlatOffsetWeb(long blocksize, short replication, int buffersize, const char * pipeorfilename, unsigned nodeid, unsigned clustercount);
    int mergeFileWeb(const char * filename, unsigned nodeid, unsigned clustercount, unsigned bufferSize, unsigned flushthreshold, short filereplication, bool deleteparts, int maxretries);
    unsigned long getTotalFilePartsSize(unsigned clustercount);

    double readTargetFileOffsetToBufferWeb(unsigned long seekPos, unsigned long readlen, int maxretries);

    int getFileStatusWeb(const char * fileurl, WebHdfsFileStatus * filestat);
    unsigned long appendBufferOffsetWeb(long blocksize, short replication, int buffersize, unsigned char * buffer);

    unsigned long getFileSizeWeb();
    unsigned long getFileSizeWeb(const char * url);

    void expandEscapedChars(const char * source, string & escaped);
    static long getRecordCount(long fsize, int clustersize, int reclen, int nodeid);

    bool hasUserName(){return hasusername;}
    bool webHdfsReached(){return webhdfsreached;}
};

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

#include "hdfsconnector.hpp"

int WebHdfs_Connector::reachWebHDFS()
{
    int retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not reach WebHDFS\n");
        return retval;
    }

    curl_easy_reset(curl);

    string filestatusstr;

    string getFileStatus;
    getFileStatus.append(baseurl);
    getFileStatus.append("?op=GETFILESTATUS");

    fprintf(stderr, "Testing connection: %s\n", getFileStatus.c_str());
    curl_easy_setopt(curl, CURLOPT_URL, getFileStatus.c_str());
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);

    //Dont' use default curl WRITEFUNCTION bc it outputs value to STDOUT.
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &filestatusstr);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
        retval = EXIT_SUCCESS;
    else
        fprintf(stderr, "Could not reach WebHDFS\n");

    return retval;
}

unsigned long WebHdfs_Connector::getFileSizeWeb(const char * fileurl)
{
    WebHdfsFileStatus filestat;

    if (getFileStatusWeb(fileurl, &filestat) != RETURN_FAILURE)
        return filestat.length;
    else
        return 0;
}

int WebHdfs_Connector::getFileStatusWeb(const char * fileurl, WebHdfsFileStatus * filestat)
{
    int retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS\n");
        return retval;
    }

    string filestatusstr;
    string requestheader;

    string getFileStatus;
    getFileStatus.append(fileurl);

    if (hasUserName())
    {
        getFileStatus.append("?user.name=");
        getFileStatus.append(username);
        getFileStatus.append("&op=GETFILESTATUS");
    }
    else
        getFileStatus.append("?op=GETFILESTATUS");

    curl_easy_reset(curl);

    fprintf(stderr, "Retrieving file status: %s\n", getFileStatus.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, getFileStatus.c_str());
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);

    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &filestatusstr);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_WRITEHEADER, &requestheader);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
    {

        try
        {
            /*
             * Not using JSON parser to avoid 3rd party deps
             */

            filestat->accessTime = -1;
            filestat->blockSize = -1;
            filestat->group = "";
            filestat->length = -1;
            filestat->modificationTime = -1;
            filestat->owner = "";
            filestat->pathSuffix = "";
            filestat->permission = "";
            filestat->replication = -1;
            filestat->type = "";

            fprintf(stderr, "%s.\n", filestatusstr.c_str());

            if (filestatusstr.find("FileStatus") >=0 )
            {
                int lenpos = filestatusstr.find("length");
                if (lenpos >= 0)
                {
                    int colpos = filestatusstr.find_first_of(':', lenpos);
                    if (colpos > lenpos)
                    {
                        int compos = filestatusstr.find_first_of(',', colpos);
                        if (compos > colpos)
                        {
                            filestat->length = atol(filestatusstr.substr(colpos+1,compos-1).c_str());
                        }
                    }
                }
            }

            retval = EXIT_SUCCESS;
        }
        catch (...) {}

        if (retval != EXIT_SUCCESS)
            fprintf(stderr, "Error fetching HDFS file status.\n");
    }
    return retval;
}

unsigned long WebHdfs_Connector::getFileSizeWeb()
{
    return targetfilestatus.length;
}

int WebHdfs_Connector::readFileOffsetWebToSTDOut(unsigned long seekPos, unsigned long readlen, int maxretries)
{
    int retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS\n");
        return retval;
    }

    curl_easy_reset(curl);
    string readfileurl;
    string requestheader;

    readfileurl.append(targetfileurl).append("?");
    if (hasUserName())
        readfileurl.append("user.name=").append(username).append("&");

    readfileurl.append("op=OPEN&offset=");
    readfileurl.append(template2string(seekPos));
    readfileurl.append("&length=");
    readfileurl.append(template2string(readlen));

    fprintf(stderr, "Reading file data: %s\n", readfileurl.c_str());

    CURLcode res;
    int failed_attempts = 0;
    double dlsize = 0;
    double tottime = 0;
    double dlspeed = 0;

    do
    {
        curl_easy_setopt(curl, CURLOPT_URL, readfileurl.c_str());
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
        curl_easy_setopt(curl, CURLOPT_VERBOSE, true);

        res = curl_easy_perform(curl);

        if (res == CURLE_OK)
        {
            curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &dlsize);
            curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &tottime);
            curl_easy_getinfo(curl, CURLINFO_SPEED_DOWNLOAD, &dlspeed);
        }
        else
        {
            failed_attempts++;
            fprintf(stderr, "Error attempting to read from HDFS file: \n\t%s\n", readfileurl.c_str());
        }
    }
    while(res != CURLE_OK && failed_attempts <= maxretries);

    if (res == CURLE_OK)
    {
        retval = EXIT_SUCCESS;
        fprintf(stderr, "\nPipe in FLAT file results:\n");
        fprintf(stderr, "Read time: %.3fsecs\t ", tottime);
        fprintf(stderr, "Read speed: %.0fbytes/sec\n", dlspeed);
        fprintf(stderr, "Read size: %.0fbytes\n", dlsize);
    }

    return retval;
}

int WebHdfs_Connector::readCSVOffsetWeb(unsigned long seekPos,
        unsigned long readlen, const char * eolseq, unsigned long bufferSize, bool outputTerminator,
        unsigned long recLen, unsigned long maxLen, const char * quote, int maxretries)
{
    fprintf(stderr, "CSV terminator: \'%s\' and quote: \'%c\'\n", eolseq, quote[0]);
    unsigned long recsFound = 0;

    unsigned eolseqlen = strlen(eolseq);
    if (seekPos > eolseqlen)
        seekPos -= eolseqlen; //read back sizeof(EOL) in case the seekpos happens to be a the first char after an EOL

    bool withinQuote = false;

    string bufferstr ="";
    bufferstr.resize(bufferSize);

    bool stopAtNextEOL = false;
    bool firstEOLfound = seekPos == 0;

    unsigned long currentPos = seekPos;

    fprintf(stderr, "--Start looking: %ld--\n", currentPos);

    unsigned long bytesLeft = readlen;

    const char * buffer;

    curl_easy_reset(curl);

    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, true);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &bufferstr);

    while(bytesLeft > 0)
    {
        bufferstr.clear();
        double num_read_bytes = readTargetFileOffsetToBufferWeb(currentPos, bytesLeft > bufferSize ? bufferSize : bytesLeft, maxretries);

        if (num_read_bytes <= 0)
        {
            fprintf(stderr, "\n--Hard Stop at: %ld--\n", currentPos);
            break;
        }

        buffer = bufferstr.c_str();

        for (int bufferIndex = 0; bufferIndex < num_read_bytes; bufferIndex++, currentPos++)
        {
            char currChar = buffer[bufferIndex];

            if (currChar == EOF)
                break;

            if (currChar == quote[0])
            {
                fprintf(stderr, "found quote char at pos: %ld\n", currentPos);
                withinQuote = !withinQuote;
            }

            if (currChar == eolseq[0] && !withinQuote)
            {
                bool eolfound = true;
                double extraNumOfBytesRead = 0;
                string tmpstr("");

                if (eolseqlen > 1)
                {
                    int eoli = bufferIndex;
                    while (eoli < num_read_bytes && eoli - bufferIndex < eolseqlen)
                    {
                        tmpstr.append(1, buffer[eoli++]);
                    }

                    if (eoli == num_read_bytes && tmpstr.size() < eolseqlen)
                    {
                        //looks like we have to do a remote read, but before we do, let's make sure the substring matches
                        if (strncmp(eolseq, tmpstr.c_str(), tmpstr.size())==0)
                        {
                            string tmpbuffer = "";
                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &tmpbuffer);
                            //TODO have to make a read... of eolseqlen - tmpstr.size is it worth it?
                            //read from the current position scanned on the file (currentPos) + number of char looked ahead for eol (eoli - bufferIndex)
                            //up to the necessary chars to determine if we're currently scanning the EOL sequence (eolseqlen - tmpstr.size()

                            extraNumOfBytesRead = readTargetFileOffsetToBufferWeb(currentPos + (eoli - bufferIndex), eolseqlen - tmpstr.size(), maxretries);
                            for(int y = 0; y < extraNumOfBytesRead; y++)
                                tmpstr.append(1, tmpbuffer.at(y));

                            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &bufferstr);
                        }
                    }

                    if (strcmp(tmpstr.c_str(), eolseq) != 0)
                        eolfound = false;
                }

                if (eolfound)
                {
                    if (!firstEOLfound)
                    {
                        bufferIndex = bufferIndex + eolseqlen - 1;
                        currentPos = currentPos + eolseqlen - 1;
                        bytesLeft = bytesLeft - eolseqlen;

                        fprintf(stderr, "\n--Start reading: %ld --\n", currentPos);

                        firstEOLfound = true;
                        continue;
                    }

                    if (outputTerminator)
                    {
                        fprintf(stdout, "%s", eolseq);

                        bufferIndex += eolseqlen;
                        currentPos += eolseqlen;
                        bytesLeft -= eolseqlen;
                    }

                    recsFound++;

                    if (stopAtNextEOL)
                    {
                        fprintf(stderr, "\n--Stop piping: %ld--\n", currentPos);
                        bytesLeft = 0;
                        break;
                    }

                    if (bufferIndex < num_read_bytes)
                        currChar = buffer[bufferIndex];
                    else
                        break;
                }
            }

            //don't pipe until we're beyond the first EOL (if offset = 0 start piping ASAP)
            if (firstEOLfound)
            {
                fprintf(stdout, "%c", currChar);
                bytesLeft--;
            }
            else
            {
                fprintf(stderr, "%c", currChar);
                bytesLeft--;
                if(maxLen > 0 && currentPos-seekPos > maxLen * 10)
                {
                    fprintf(stderr, "\nFirst EOL was not found within the first %ld bytes", currentPos-seekPos);
                    return EXIT_FAILURE;
                }
            }

            if (stopAtNextEOL)
                fprintf(stderr, "%c", currChar);

            // If bytesLeft <= 0 at this point, but he haven't encountered
            // the last EOL, need to continue piping untlil EOL is encountered.
            if (bytesLeft <= 0  && currChar != eolseq[0])
            {
                if(!firstEOLfound)
                {
                    fprintf(stderr, "\n--Reached end of readlen before finding first record start at: %ld (breaking out)--\n",  currentPos);
                    break;
                }

                if (stopAtNextEOL)
                {
                    fprintf(stderr, "Could not find last EOL, breaking out a position %ld\n", currentPos);
                    break;
                }

                fprintf(stderr, "\n--Looking for Last EOL: %ld --\n", currentPos);
                bytesLeft = maxLen; //not sure how much longer until next EOL read up max record len;
                stopAtNextEOL = true;
            }
        }
    }

    fprintf(stderr, "\nCurrentPos: %ld, RecsFound: %ld\n", currentPos, recsFound);

    return EXIT_SUCCESS;
}

double WebHdfs_Connector::readTargetFileOffsetToBufferWeb(unsigned long seekPos, unsigned long readlen, int maxretries)
{
    double returnval = 0;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS");
        return returnval;
    }

    char readfileurl [1024];
    if (hasUserName())
        sprintf(readfileurl, "%s?user.name=%s&op=OPEN&offset=%lu&length=%lu",targetfileurl.c_str(), username.c_str(), seekPos,readlen);
    else
        sprintf(readfileurl, "%s?op=OPEN&offset=%lu&length=%lu",targetfileurl.c_str(), seekPos,readlen);

    curl_easy_setopt(curl, CURLOPT_URL, readfileurl);

    CURLcode res;
    int failed_attempts = 0;
    do
    {
        res = curl_easy_perform(curl);

        if (res == CURLE_OK)
        {
            double dlsize;
            curl_easy_getinfo(curl, CURLINFO_SIZE_DOWNLOAD, &dlsize);
            if (dlsize > readlen)
                fprintf(stderr, "Warning: received incorrect number of bytes from HDFS.\n");
            else
                returnval = dlsize;
        }
        else
        {
            failed_attempts++;
            fprintf(stderr, "Error attempting to read from HDFS file: \n\t%s\n", readfileurl);
        }
    }
    while(res != CURLE_OK && failed_attempts <= maxretries);

    return returnval;
}

int WebHdfs_Connector::writeFlatOffsetWeb(long blocksize, short replication, int buffersize, const char * pipeorfilename, unsigned nodeid, unsigned clustercount)
{
    int retval = RETURN_FAILURE;

    if (strlen(pipeorfilename) <= 0)
    {
        fprintf(stderr, "Bad data pipe or file name found");
        return retval;
    }

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS");
        return retval;
    }

    curl_easy_reset(curl);

    char openfileurl [1024];
    if (hasUserName())
        sprintf(openfileurl, "%s-parts/part_%d_%d?user.name=%s&op=CREATE&replication=%d&overwrite=true",targetfileurl.c_str(),nodeid, clustercount,username.c_str(), 1);
    else
        sprintf(openfileurl, "%s-parts/part_%d_%d?op=CREATE&replication=%d&overwrite=true",targetfileurl.c_str(),nodeid, clustercount, 1);

    _IO_FILE * datafileorpipe;
    datafileorpipe = fopen(pipeorfilename, "rb");

    fprintf(stderr, "Setting up new HDFS file: %s\n", openfileurl);

    curl_easy_setopt(curl, CURLOPT_READFUNCTION, continueCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_READDATA, datafileorpipe);
    curl_easy_setopt(curl, CURLOPT_URL, openfileurl);
    curl_easy_setopt(curl, CURLOPT_PUT, true);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, true);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, false);

    string header;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl,   CURLOPT_WRITEHEADER, &header);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
    {
        size_t found;
        found = header.find_first_of("307 TEMPORARY_REDIRECT");
//        HTTP/1.1 100 Continue\\r\\n\\r\\nHTTP/1.1 307 TEMPORARY_REDIRECT\\r\\nContent-Type: application/json\\r\\nExpires: Thu, 01-Jan-1970 00:00:00 GMT\\r\\nSet-Cookie: hadoop.auth=\\"u=hadoop&p=hadoop&t=simple&e=1345504538799&s=
        if (found!=string::npos)
        {
            found=header.find("Location:",found+22);
            if (found!=string::npos)
            {
                size_t  eolfound =header.find(EOL,found);
                string tmp = header.substr(found+10,eolfound-(found+10) - strlen(EOL));
                fprintf(stderr, "Redirect location: %s\n", tmp.c_str());

                curl_easy_setopt(curl, CURLOPT_URL, tmp.c_str());
                curl_easy_setopt(curl, CURLOPT_UPLOAD, true);
                curl_easy_setopt(curl, CURLOPT_READFUNCTION, readFileCallBackCurl);
                curl_easy_setopt(curl, CURLOPT_READDATA, datafileorpipe);

                string errorbody;
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
                curl_easy_setopt(curl,   CURLOPT_WRITEDATA, &errorbody);

                res = curl_easy_perform(curl);
                if (res == CURLE_OK)
                {
                    if (errorbody.length() > 0)
                        fprintf(stderr, "Error transferring file: %s\n", errorbody.c_str());
                    else
                        retval = EXIT_SUCCESS;
                }
                else
                {
                    fprintf(stderr, "Error transferring file. Curl error code: %d\n", res);
                }
            }
        }
    }
    else
    {
        fprintf(stderr, "Error setting up file: %s.\n", openfileurl);
    }

    return retval;
}

unsigned long WebHdfs_Connector::getTotalFilePartsSize(unsigned clustercount)
{
    unsigned long totalSize = 0;

    for (unsigned node = 0; node < clustercount; node++)
    {
        char filepartname[1024];
        memset(&filepartname[0], 0, sizeof(filepartname));
        sprintf(filepartname,"%s-parts/part_%d_%d", targetfileurl.c_str(), node, clustercount);

        unsigned long partFileSize = getFileSizeWeb(filepartname);

        if (partFileSize <= 0)
        {
            fprintf(stderr,"Error: Could not find part file: %s", filepartname);
            return 0;
        }

        totalSize += partFileSize;
    }

    return totalSize;
}

unsigned long WebHdfs_Connector::appendBufferOffsetWeb(long blocksize, short replication, int buffersize, unsigned char * buffer)
{
    //curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"

    unsigned long retval = RETURN_FAILURE;

    if (!curl)
    {
        fprintf(stderr, "Could not connect to WebHDFS");
        return retval;
    }

    curl_easy_reset(curl);

    char openfileurl [1024];
    if (hasUserName())
        sprintf(openfileurl, "%s?user.name=%s&op=APPEND&buffersize=%d",targetfileurl.c_str(), username.c_str(), buffersize);
    else
        sprintf(openfileurl, "%s?op=APPEND&buffersize=%d",targetfileurl.c_str(), buffersize);

    curl_easy_setopt(curl, CURLOPT_READFUNCTION, continueCallBackCurl);
    curl_easy_setopt(curl, CURLOPT_URL, openfileurl);
    curl_easy_setopt(curl, CURLOPT_POST, true);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, false);

    string header;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
    curl_easy_setopt(curl,   CURLOPT_WRITEHEADER, &header);

    CURLcode res = curl_easy_perform(curl);

    if (res == CURLE_OK)
    {
        size_t found;
        found = header.find("307 TEMPORARY_REDIRECT");

        if (found!=string::npos)
        {
            found=header.find("Location:",found+22);
            if (found!=string::npos)
            {
                size_t  eolfound =header.find(EOL,found);
                string tmp = header.substr(found+10,eolfound-(found+10) - strlen(EOL));

                curl_easy_setopt(curl, CURLOPT_URL, tmp.c_str());

                curl_easy_setopt(curl, CURLOPT_POST, true);
                curl_easy_setopt(curl, CURLOPT_READFUNCTION, readBufferCallBackCurl);
                curl_easy_setopt(curl, CURLOPT_READDATA, buffer);
                curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, buffersize);
                curl_easy_setopt(curl, CURLOPT_VERBOSE, false);

                header.clear();
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToStrCallBackCurl);
                curl_easy_setopt(curl,   CURLOPT_WRITEHEADER, &header);

                res = curl_easy_perform(curl);

                fprintf(stderr, "Response: %s\n", header.c_str());
            }
        }
    }

    if (res != CURLE_OK)
        fprintf(stderr, "Error transferring file. Curl error code: %d\n", res);
    else
        retval = buffersize;

    return retval;
}

void WebHdfs_Connector::expandEscapedChars(const char * source, string & escaped)
{
    int si = 0;

    while (source[si])
    {
        if (source[si] == '\\')
        {
            switch (source[++si])
            {
            case 'n':
                escaped.append(1, '\n');
                break;
            case 'r':
                escaped.append(1, '\r');
                break;
            case 't':
                escaped.append(1, '\t');
                break;
            case 'b':
                escaped.append(1, '\b');
                break;
            case 'v':
                escaped.append(1, '\v');
                break;
            case 'f':
                escaped.append(1, '\f');
                break;
            case '\\':
                escaped.append(1, '\\');
                break;
            case '\'':
//fprintf(stderr, "adding escaped single quote..");
                escaped.append(1, '\'');
                break;
            case '\"':
                escaped.append(1, '\"');
                break;
            case '0':
                escaped.append(1, '\0');
                break;
//			case 'c':
//				escaped.append(1,'\c');
//				break;
            case 'a':
                escaped.append(1, '\a');
                break;
//			case 's':
//				escaped.append(1,'\s');
//				break;
            case 'e':
                escaped.append(1, '\e');
                break;
            default:
                break;

            }
        }
        else
            escaped.append(1, source[si]);

        si++;
    }
}

int main(int argc, char **argv)
{
    WebHdfs_Connector * connector = NULL;

    unsigned int bufferSize = 1024 * 100;
    unsigned int flushThreshold = bufferSize * 10;
    int returnCode = EXIT_FAILURE;
    unsigned clusterCount = 0;
    unsigned nodeID = 0;
    unsigned long recLen = 0;
    unsigned long maxLen = 0;
    unsigned long dsLen = 0;
    const char * fileName = "";
    const char * hadoopHost = "default";
    const char * hadoopPort = "0";

    string format("");
    string foptions("");
    string data("");

    int currParam = 1;

    const char * wuid = "";
    const char * rowTag = "Row";
    const char * separator = "";
    string terminator(EOL);
    bool outputTerminator = true;
    string quote("'");
    const char * headerText = "<Dataset>";
    const char * footerText = "</Dataset>";
    const char * hdfsuser = "";
    const char * hdfsgroup = "";
    const char * pipepath = "";
    bool cleanmerge = false;
    bool verbose = false;
    short filereplication = 1;
    unsigned short whdfsretrymax = 1;

    int blocksize = 0;
    int replication = 1;

    enum HDFSConnectorAction
    {
        HCA_INVALID = -1,
        HCA_STREAMIN = 0,
        HCA_STREAMOUT = 1,
        HCA_STREAMOUTPIPE = 2,
        HCA_READOUT = 3,
        HCA_MERGEFILE = 4
    };

    HDFSConnectorAction action = HCA_INVALID;

    while (currParam < argc)
    {
        if (strcmp(argv[currParam], "-si") == 0)
        {
            action = HCA_STREAMIN;
        }
        else if (strcmp(argv[currParam], "-so") == 0)
        {
            action = HCA_STREAMOUT;
        }
        else if (strcmp(argv[currParam], "-sop") == 0)
        {
            action = HCA_STREAMOUTPIPE;
        }
        else if (strcmp(argv[currParam], "-mf") == 0)
        {
            action = HCA_MERGEFILE;
        }
        else if (strcmp(argv[currParam], "-clustercount") == 0)
        {
            clusterCount = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-nodeid") == 0)
        {
            nodeID = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-reclen") == 0)
        {
            recLen = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-format") == 0)
        {
            const char * tmp = argv[++currParam];
            while (*tmp && *tmp != '(')
                format.append(1, *tmp++);
            fprintf(stderr, "Format: %s\n", format.c_str());
            if (*tmp++)
                while (*tmp && *tmp != ')')
                    foptions.append(1, *tmp++);
        }
        else if (strcmp(argv[currParam], "-rowtag") == 0)
        {
            rowTag = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-filename") == 0)
        {
            fileName = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-host") == 0)
        {
            hadoopHost = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-port") == 0)
        {
            hadoopPort = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-wuid") == 0)
        {
            wuid = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-data") == 0)
        {
            data.append(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-separator") == 0)
        {
            separator = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-terminator") == 0)
        {
            terminator.clear();
            connector->expandEscapedChars(argv[++currParam], terminator);
        }
        else if (strcmp(argv[currParam], "-quote") == 0)
        {
            quote.clear();
            connector->expandEscapedChars(argv[++currParam], quote);
        }
        else if (strcmp(argv[currParam], "-headertext") == 0)
        {
            headerText = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-footertext") == 0)
        {
            footerText = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-buffsize") == 0)
        {
            bufferSize = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-outputterminator") == 0)
        {
            outputTerminator = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-maxlen") == 0)
        {
            maxLen = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-hdfsuser") == 0)
        {
            hdfsuser = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-hdfsgroup") == 0)
        {
            hdfsgroup = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-pipepath") == 0)
        {
            pipepath = argv[++currParam];
        }
        else if (strcmp(argv[currParam], "-flushsize") == 0)
        {
            flushThreshold = atol(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-cleanmerge") == 0)
        {
            cleanmerge = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-hdfsfilereplication") == 0)
        {
            filereplication = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-whdfsretrymax") == 0)
        {
           whdfsretrymax = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-verbose") == 0)
        {
           verbose = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-blocksize") == 0)
        {
            blocksize = atoi(argv[++currParam]);
        }
        else if (strcmp(argv[currParam], "-replication") == 0)
        {
            replication = atoi(argv[++currParam]);
        }
        else
        {
            fprintf(stderr, "Error: Found invalid input param: %s \n", argv[currParam]);
            return returnCode;
        }
        currParam++;
    }

    connector = new WebHdfs_Connector(hadoopHost, hadoopPort, hdfsuser, fileName);

    if (connector->webHdfsReached())
    {
        if (action == HCA_STREAMIN)
        {
            fprintf(stderr, "\nStreaming in %s...\n", fileName);

            unsigned long fileSize = connector->getFileSizeWeb();

            if (fileSize != RETURN_FAILURE)
            {
                if (strcmp(format.c_str(), "FLAT") == 0)
                {
                    unsigned long recstoread = getRecordCount(fileSize, clusterCount, recLen, nodeID);
                    if (recstoread != RETURN_FAILURE)
                    {
                        unsigned long totRecsInFile = fileSize / recLen;
                        unsigned long offset = nodeID * (totRecsInFile / clusterCount) * recLen;
                        unsigned long leftOverRecs = totRecsInFile % clusterCount;

                        if (leftOverRecs > 0)
                        {
                            if (leftOverRecs > nodeID)
                                offset += nodeID * recLen;
                            else
                                offset += leftOverRecs * recLen;
                        }

                        fprintf(stderr, "fileSize: %lu offset: %lu size bytes: %lu, recstoread:%lu\n", fileSize, offset,
                                recstoread * recLen, recstoread);

                        if (offset < fileSize)
                            returnCode = connector->readFileOffsetWebToSTDOut(offset, recstoread * recLen, whdfsretrymax);
                    }
                    else
                        fprintf(stderr, "Could not determine number of records to read");
                }

                else if (strcmp(format.c_str(), "CSV") == 0)
                {
                    fprintf(stderr, "Filesize: %ld, Offset: %ld, readlen: %ld\n", fileSize,
                            (fileSize / clusterCount) * nodeID, fileSize / clusterCount);

                    returnCode = connector->readCSVOffsetWeb((fileSize / clusterCount) * nodeID,
                            fileSize / clusterCount, terminator.c_str(), bufferSize, outputTerminator, recLen, maxLen,
                            quote.c_str(), whdfsretrymax);
                }
                else
                    fprintf(stderr, "Unknown format type: %s(%s)", format.c_str(), foptions.c_str());
            }
            else
                fprintf(stderr, "Could not determine HDFS file size: %s", fileName);
        }
        else if (action == HCA_STREAMOUT || action == HCA_STREAMOUTPIPE)
        {
            returnCode = connector->writeFlatOffsetWeb(blocksize, replication, bufferSize, pipepath, nodeID, clusterCount);
        }
        else if (action == HCA_MERGEFILE)
            fprintf(stderr, "Merging of file parts deprecated.\n");
        else
            fprintf(stderr, "\nNo action type detected, exiting.\n");

        if (connector)
            delete connector;
    }
    else
        fprintf(stderr, "Could not connect to HDFS on %s:%s\n", hadoopHost, hadoopPort);

    return returnCode;
}

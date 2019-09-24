package main

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// var conn net.Conn

// var listener net.Listener

const ftpTimeTemplate = "20060102150405"

const configNode = "default"

var basePath string

var connectionPool GenericPool

var monitorConnection net.Conn

//主机地址
var serverHost string

//主机端口
var serverPort string

//用户名
var user string

//密码
var password string

//主机目录
var serverDir string

// 扩展目录
var extDirMap = make(map[string]string)

//本机目录
var localDir string

//最大并发数
var maxThread string

var downloadJobChan = make(chan string, 1)

var scanTimes int64

var downloadTimes int64

var downloadJobMap sync.Map

var exit = make(chan int)

var wg sync.WaitGroup

var du, err = time.ParseDuration("10s")

func main() {
	// 初始化配置文件
	basePath, _ = filepath.Abs(filepath.Dir(os.Args[0]))
	ftpConfig := new(FtpConfig)
	ftpConfig.Init(fmt.Sprintf("%v/config.ini", basePath))
	serverHost = ftpConfig.Read(configNode, "host")
	serverPort = ftpConfig.Read(configNode, "port")
	user = ftpConfig.Read(configNode, "user")
	password = ftpConfig.Read(configNode, "password")
	serverDir = ftpConfig.Read(configNode, "serverDir")
	excludeDir := ftpConfig.Read(configNode, "excludeDir")
	if excludeDir != "" {
		for _, dir := range strings.Split(excludeDir, ";") {
			extDirMap[dir] = "0"
		}
	}
	localDir = ftpConfig.Read(configNode, "localDir")
	maxThread = ftpConfig.Read(configNode, "maxThread")
	// 初始化连接池
	threadNum, _ := strconv.Atoi(maxThread)
	genericPool, err := InitConnectionPool(threadNum, ConnectionFactory)
	connectionPool = *genericPool
	// 初始化一个监听链接
	monitorConnection, _ = ConnectionFactory()

	if err != nil {
		Logf("初始化连接池失败", err)
		return
	}
	// 初始化本地目录
	_, err = os.Stat(localDir)
	if err != nil {
		err = os.MkdirAll(strings.TrimRight(localDir, "/"), os.ModePerm)
		if err != nil {
			Logf("本地目录创建失败%v", err)
		}
	}
	//ticker := time.NewTicker(time.Second * 5)
	//go func() {
	//	for _ = range ticker.C {
	//		log.Printf("---")
	//		SynchronousFiles(serverDir, localDir)
	//	}
	//}()
	// 启动监听程序
	go StartFileChangeMonitor(3)
	// 启动下载任务
	go StartDownloadJob()

	<-exit

	connectionPool.Shutdown()

}

// 启动监听文件变化
func StartFileChangeMonitor(second int) {
	lock := make(chan int, 1)
	for {
		lock <- 1
		time.AfterFunc(time.Duration(second)*time.Second, func() {
			// log.Println("监听文件")
			//重置计数器
			atomic.StoreInt64(&scanTimes, 0)
			err := CheckConn(monitorConnection)
			if err != nil {
				monitorConnection.Close()
				monitorConnection, err = ConnectionFactory()
				if err != nil {
					return
				}
			} else {
				SynchronousFiles(serverDir, localDir)
			}

			// wg.Wait()
			// log.Println("----")
			<-lock
		})
	}
}

// 启动下载任务
func StartDownloadJob() {
	//设置下载计数器
	atomic.StoreInt64(&downloadTimes, 0)
	for {
		downloadJobMap.Range(func(key, value interface{}) bool {
			// t.Logf("range k:%v,v=%v\n", key, value)
			files := strings.Split(fmt.Sprintf("%v", value), "|")
			downloadJobMap.Store(key, "0")
			if len(files) == 2 {
				conn, _ := connectionPool.Acquire()
				// wg.Add(1)

				go DownloadFile(conn, files[0], files[1])
			}
			return true
		})
		// wg.Wait()
		time.Sleep(time.Duration(2) * time.Second)
	}
	//for job := range downloadJobChan {
	//	files := strings.Split(job, "|")
	//	if len(files) == 2 {
	//		conn, _ := connectionPool.Acquire()
	//		go DownloadFile(conn, files[0], files[1])
	//	}
	//}
}

//递归同步文件
func SynchronousFiles(serverPath string, localPath string) {
	_, ok := extDirMap[serverPath]
	if ok {
		return
	}
	atomic.AddInt64(&scanTimes, 1)
	fileInfos := GetFileList(serverPath)
	temp1 := strconv.FormatInt(atomic.LoadInt64(&scanTimes), 10)
	temp2 := strconv.FormatInt(atomic.LoadInt64(&downloadTimes), 10)

	Logf("[%v,%v]扫描目录->%v", temp1, temp2, serverPath)
	if len(fileInfos) == 0 {
		return
	}
	// log.Printf("%v有%d个文件待处理", serverPath, len(fileInfos))
	for _, fileInfo := range fileInfos {
		serverFilePath := serverPath + fileInfo.fileName
		localFilePath := localPath + fileInfo.fileName
		jobKey := serverFilePath
		jobVal := fmt.Sprintf("%v|%v", serverFilePath, localFilePath)
		_, ok := downloadJobMap.Load(jobKey)
		if ok {
			// Logf("%v已在队列中", serverPath)
			continue
		}

		file, err := os.Stat(localFilePath)
		if !fileInfo.isDir {
			if err == nil {
				if fileInfo.fileSize == file.Size() {
					continue
				} else {
					oldLocalFilePath := localFilePath + strconv.FormatInt(time.Now().Unix(), 10) + ".bak"
					// log.Printf("文件%v已存在，但与服务器大小不一致，将文件备份为%v后重新下载", localFilePath, oldLocalFilePath)
					os.Rename(localFilePath, oldLocalFilePath)
				}
			}

			//if fileInfo.fileSize < 1024*1024 {
			//conn, err := connectionPool.Acquire()
			//if err != nil {
			//	log.Printf("获取连接失败%v", err)
			//	return
			//}
			// wg.Add(1)
			// go DownloadFile(conn, serverFilePath, localFilePath)

			// fmt.Printf("检测到新文件:%v", serverFilePath)
			// log.Printf("\n%v->加入队列\r\n", serverFilePath)
			Logf("加入队列->%v", serverFilePath)
			downloadJobMap.Store(jobKey, jobVal)
			atomic.AddInt64(&downloadTimes, 1)
			// downloadJobChan <- jobKey
			//}

		} else {
			if err != nil {
				_ = os.Mkdir(localFilePath, os.ModePerm)
			}
			SynchronousFiles(serverPath+fileInfo.fileName+"/", localFilePath+"/")
		}
	}

}

//创建连接工厂
func ConnectionFactory() (conn net.Conn, err error) {
	buf := make([]byte, 512)

	du, err := time.ParseDuration("10s")
	if err != nil {
		Logf("ParseDuration:", err)
		return
	}
	serverAddress := fmt.Sprintf("%v:%v", serverHost, serverPort)
	// log.Printf("连接到主机:%v", serverAddress)

	conn, err = net.DialTimeout("tcp", serverAddress, du)
	if err != nil {
		Logf("Dial:", err)
		return
	}
	n, err := conn.Read(buf)
	if err != nil {
		Logf("Read:", err)
		return nil, err
	}

	code, _ := strconv.Atoi(string(buf[0:3]))
	if code != 220 {
		Logf("Read:", string(buf[:n]))
		return nil, errors.New(string(buf[:n]))
	}
	buf, _ = ExecCMD(conn, "USER "+user+"\r\n")
	code, _ = strconv.Atoi(string(buf[0:3]))
	if code != 331 {
		Logf("USER:", string(buf))
		return nil, errors.New(string(buf[:n]))
	}
	buf, _ = ExecCMD(conn, "PASS "+password+"\r\n")
	code, _ = strconv.Atoi(string(buf[0:3]))
	if code != 230 {
		Logf("PASS:", string(buf))
		return nil, errors.New(string(buf[:n]))
	}
	// ExecCMD(conn, "CWD /\r\n")
	return conn, nil
}

// 自定义配置文件对象
const middle = "========="

type FtpConfig struct {
	PropMap map[string]string
	strcet  string
}

//初始化配置文件
func (c *FtpConfig) Init(configPath string) {
	c.PropMap = make(map[string]string)
	_, err := os.Stat(configPath)
	if err != nil {
		Logf("配置文件%v不存在;%v", configPath, err)
		os.Create(configPath)
		return
	}
	file, err := os.Open(configPath)
	defer file.Close()

	r := bufio.NewReader(file)

	for {
		b, _, err := r.ReadLine()
		if err != nil {
			break
			// panic(err)
		}
		s := strings.TrimSpace(string(b))
		//fmt.Println(s)
		if strings.Index(s, "#") == 0 {
			continue
		}

		n1 := strings.Index(s, "[")
		n2 := strings.LastIndex(s, "]")
		if n1 > -1 && n2 > -1 && n2 > n1+1 {
			c.strcet = strings.TrimSpace(s[n1+1 : n2])
			continue
		}

		if len(c.strcet) == 0 {
			continue
		}
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}

		frist := strings.TrimSpace(s[:index])
		if len(frist) == 0 {
			continue
		}
		second := strings.TrimSpace(s[index+1:])

		pos := strings.Index(second, "\t#")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " #")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, "\t//")
		if pos > -1 {
			second = second[0:pos]
		}

		pos = strings.Index(second, " //")
		if pos > -1 {
			second = second[0:pos]
		}

		if len(second) == 0 {
			continue
		}

		key := c.strcet + middle + frist
		c.PropMap[key] = strings.TrimSpace(second)
	}
}

func (c FtpConfig) Read(node, key string) string {
	key = node + middle + key
	v, found := c.PropMap[key]
	if !found {
		return ""
	}
	return v
}

// 自定义文件对象
type FileInfo struct {
	fileName   string
	fileSize   int64
	isDir      bool
	modifyTime int64
}

func GetFileList(path string) (fileInfos []FileInfo) {
	//conn, err := connectionPool.Acquire()
	//if err != nil {
	//	log.Printf("获取连接失败%v", err)
	//}
	ExecCMD(monitorConnection, "CWD "+path+"\r\n")
	// ExecCMD(monitorConnection, "PWD "+path+"\r\n")
	ExecPASV(monitorConnection, "MLSD\r\n", func(dataConn net.Conn) {
		fileConent := ""
		for {
			dataBuf := make([]byte, 1024)
			n, err := dataConn.Read(dataBuf)
			if err == io.EOF || err != nil {
				// log.Println("Read:", err)
				break
			}
			fileConent += string(dataBuf[:n])
		}
		files := strings.Split(fileConent, "\r\n")
		for _, file := range files {
			if len(file) > 0 {
				//fmt.Printf("%v %v\r\n", strconv.Itoa(i), file)
				temps := strings.Split(file, ";")

				if temps[0] == "type=dir" {
					modifyTime, _ := time.Parse(ftpTimeTemplate, strings.Split(temps[1], "=")[1])
					tempFileInfo := FileInfo{fileName: strings.Trim(temps[2], " "), modifyTime: modifyTime.Unix(), isDir: true, fileSize: 0}
					fileInfos = append(fileInfos, tempFileInfo)
				} else if temps[0] == "type=file" {
					modifyTime, _ := time.Parse(ftpTimeTemplate, strings.Split(temps[1], "=")[1])
					size, _ := strconv.ParseInt(strings.Split(temps[2], "=")[1], 10, 64)
					tempFileInfo := FileInfo{fileName: strings.Trim(strings.Join(temps[3:len(temps)], ";"), " "), modifyTime: modifyTime.Unix(), isDir: false, fileSize: size}
					fileInfos = append(fileInfos, tempFileInfo)
				}

			}
		}
	})
	// connectionPool.Release(conn)
	return fileInfos
}

func DownloadFile(conn net.Conn, serverFilePath string, localFilePath string) {

	defer func() {
		atomic.AddInt64(&downloadTimes, -1)
		downloadJobMap.Delete(serverFilePath)
		if r := recover(); r != nil {
			Logf("DownloadFile：%s\n", r)
		}
		connectionPool.Release(conn)
	}()

	// defer wg.Done()

	contentSize := 0
	ExecPASV(conn, "RETR "+serverFilePath+"\r\n", func(dataConn net.Conn) {
		file, _ := os.Create(localFilePath)
		defer file.Sync()
		defer file.Close()

		tempTimes := 0
		fmt.Fprintf(os.Stdout, "开始下载 %v\r", serverFilePath)
		for {
			// fmt.Print(".")
			dataBuf := make([]byte, 4096) //  4096
			n, err := dataConn.Read(dataBuf)
			if err == io.EOF {
				break
			}
			if err != nil {
				Logf("Data Read Error :%v", err)
				break
			}
			file.Write(dataBuf[:n])
			contentSize += n
			// fmt.Printf("\033[%dA\033[%dB", 2, 2)

			tempTimes++
			if tempTimes%20 == 0 {
				file.Sync()
				fmt.Fprintf(os.Stdout, "[%v] %v已下载:%dKB \r", os.Getpid(), serverFilePath, contentSize)
			}
		}

		Logf("下载完成->%v %dKB \r", serverFilePath, contentSize)
	})
}

type Callback func(conn net.Conn)

func ExecPASV(conn net.Conn, cmd string, callback Callback) {

	defer func() {
		if r := recover(); r != nil {
			Logf("ExecPASV：%s\n", r)
			// connectionPool.Close(conn)
		}
	}()

	buf, n := ExecCMD(conn, "PASV\r\n")
	code, err := strconv.Atoi(string(buf[0:3]))
	if code != 227 || err != nil {
		time.Sleep(time.Duration(2) * time.Second)
		// ExecPASV(conn, cmd, callback)
		Logf("Cmd:", string(buf[:n]))
		return
	}
	msg := string(buf[27 : n-3])
	list := strings.Split(msg, ",")
	n1, err := strconv.Atoi(list[len(list)-2])
	if err != nil {
		Logf("Read:", err)
		return
	}
	n2, err := strconv.Atoi(list[len(list)-1])
	if err != nil {
		Logf("Read:", err)
		return
	}
	port := n1*256 + n2 // ????????为什么这么计算呢
	du, err := time.ParseDuration("10s")
	dataCon, err := net.DialTimeout("tcp", fmt.Sprintf("%v:%d", serverHost, port), du)
	defer func() {
		if dataCon != nil {
			dataCon.Close()
		}
	}()

	if dataCon == nil || err != nil {
		if err != nil {
			log.Println("Connection:", err)
		}
		time.Sleep(time.Duration(2) * time.Second)
		// ExecPASV(conn, cmd, callback)
		return
	}

	ExecCMD(conn, cmd)
	if callback != nil {
		callback(dataCon)
		// 这段很重要----------------------
		temp := make([]byte, 512)
		_, _ = conn.Read(temp)
		_ = string(temp[:n])
		// log.Printf("download ok:%v", m)
		// 这段很重要----------------------

	}

}

func ExecCMD(conn net.Conn, cmd string) (buff []byte, n int) {

	buf := make([]byte, 512)

	_, err := conn.Write([]byte(cmd))
	if err != nil {
		log.Println("Write:", err)
		return
	}

	len, err := conn.Read(buf)
	if err != nil {
		log.Println("Read:", err)
		// connectionPool.Close(conn)
		return
	}
	return buf[:len], len
}

type factory func() (conn net.Conn, err error)

type Pool interface {
	Acquire() (conn net.Conn) // 获取资源
	Release(io.Closer) error  // 释放连接
	Close(io.Closer) error    // 关闭连接
	Shutdown() error          // 关闭连接池
}

type GenericPool struct {
	sync.Mutex
	pool    chan net.Conn
	maxOpen int
	numOpen int
	closed  bool
	factory factory //创建连接
}

//初始化连接池
func InitConnectionPool(maxOpen int, factory factory) (*GenericPool, error) {
	if maxOpen <= 0 {
		return nil, errors.New("连接数量不能小于等于0")
	}

	p := &GenericPool{
		pool:    make(chan net.Conn, maxOpen),
		maxOpen: maxOpen,
		closed:  false,
		factory: factory,
	}
	for i := 0; i < maxOpen; i++ {
		closer, err := factory()
		if err != nil {
			continue
		}
		p.numOpen++
		p.pool <- closer
	}
	return p, nil
}

//获取连接
func (p *GenericPool) getConn() (conn net.Conn, err error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	default:
	}
	// p.Lock()
	if p.numOpen >= p.maxOpen {
		conn := <-p.pool
		// p.Unlock()
		return conn, nil
	}
	conn, err = p.factory()
	if err != nil {
		// p.Unlock()
		return nil, err
	}
	p.numOpen++
	//p.Unlock()
	return conn, nil
}

// 释放连接，将连接放回连接池
func (p *GenericPool) Release(conn net.Conn) error {

	if p.closed {
		return nil //errors.New("连接已关闭")
	}
	// p.Lock()
	p.pool <- conn
	// p.Unlock()
	return nil
}

// 关闭连接
func (p *GenericPool) Close(closer io.Closer) error {
	//p.Lock()
	// p.closed = true
	closer.Close()
	p.numOpen--
	//p.Unlock()
	return nil
}

// 关闭连接池
func (p *GenericPool) Shutdown() error {
	if p.closed {
		return errors.New("连接已关闭")
	}
	p.Lock()
	close(p.pool)
	for closer := range p.pool {
		closer.Close()
		p.numOpen--
	}
	p.closed = true
	p.Unlock()
	return nil
}

// 获取连接池中的连接
func (p *GenericPool) Acquire() (conn net.Conn, err error) {

	if p.closed {
		return nil, errors.New("连接已关闭")
	}
	for {
		conn, err := p.getConn()
		if err != nil {
			return nil, err
		}
		err = CheckConn(conn)
		if err == nil {
			return conn, nil
		} else {
			p.Close(conn)
		}
	}
}

//检测连接状态
func CheckConn(conn net.Conn) error {
	buf := make([]byte, 512)
	_, err := conn.Write([]byte("NOOP\r\n"))
	if err != nil {
		Logf("check conn err:%v:", err)
		return err
	}
	_, err = conn.Read(buf)
	if err != nil {
		Logf("check conn err:%v:", err)
		return err
	}
	return nil
}

var t = "2006-01-02 15:04:05"

func Logf(mess string, a ...interface{}) {
	time := time.Now()
	temp := fmt.Sprintf("[%v][%v]", os.Getpid(), time.Format(t)) + mess + "\r\n"
	fmt.Printf(temp, a...)
	//f, _ := os.OpenFile(basePath+"/log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//_, _ = io.WriteString(f, fmt.Sprintf(temp, a...))
	//defer f.Close()
}

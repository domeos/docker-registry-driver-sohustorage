package storage

import (
	"errors"
	"io"
	"sort"
	"log"
	"time"
	"fmt"
	"os"
	"bufio"
	"strings"

	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/mysql"
	//"github.com/docker/distribution/registry/storage/driver"
)

// ErrFinishedWalk is used when the called walk function no longer wants
// to accept any more values.  This is used for pagination when the
// required number of repos have been found.
var ErrFinishedWalk = errors.New("finished walk")

// Returns a list, or partial list, of repositories in the registry.
// Because it's a quite expensive operation, it should only be used when building up
// an initial set of repositories.
func (reg *registry) Repositories(ctx context.Context, repos []string, last string) (n int, errVal error) {
	var foundRepos []string

	if len(repos) == 0 {
		return 0, errors.New("no space in slice")
	}

	/*
	err = Walk(ctx, reg.blobStore.driver, root, func(fileInfo driver.FileInfo) error {
		filePath := fileInfo.Path()

		// lop the base path off
		repoPath := filePath[len(root)+1:]

		_, file := path.Split(repoPath)
		if file == "_layers" {
			repoPath = strings.TrimSuffix(repoPath, "/_layers")
			if repoPath > last {
				foundRepos = append(foundRepos, repoPath)
			}
			return ErrSkipDir
		} else if strings.HasPrefix(file, "_") {
			return ErrSkipDir
		}

		// if we've filled our array, no need to walk any further
		if len(foundRepos) == len(repos) {
			return ErrFinishedWalk
		}

		return nil
	})
	*/

	// Query
	if os.Getenv("SEARCH_BACKEND") == "MYSQL" {
		DB := mysql.InitDatabase()

		if DB != nil {
			rows, err := DB.Query("select name from repositories")
			if err != nil {
				log.Println("select name from repositories fail: ", err)
			} else {
				for rows.Next() {
					var repoPath string
					err = rows.Scan(&repoPath)
					if err != nil {
						log.Println("scan fail: ", err)
						continue;
					}
					if repoPath > last {
						foundRepos = append(foundRepos, repoPath)
					}
				}
			}
			DB.Close()
		}
	} else if os.Getenv("SEARCH_BACKEND") == "LOCAL" {
		file, err := os.Open("/opt/registry/repositories")
		if err != nil {
			return 0, err
		}

		buf := bufio.NewReader(file)
		for {
			repoPath, err := buf.ReadString('\n')
			if err != nil && err != io.EOF {
				break
			}
			repoPath = strings.TrimSpace(repoPath)
			if repoPath > last {
				foundRepos = append(foundRepos, repoPath)
			}
			if err == io.EOF {
				break
			}
		}
	} else {
		root, err := pathFor(repositoriesRootPathSpec{})
		if err != nil {
			return 0, err
		}

		catalogRoot := "/catalog" + root
		children, err := reg.blobStore.driver.List(ctx, catalogRoot)
		if err != nil {
			return 0, err
		}

		for _, child := range children {
			repoPath := child[len(root)+1:]
			if repoPath > last {
				foundRepos = append(foundRepos, repoPath)
			}
		}
	}

	sort.Strings(foundRepos)

	n = copy(repos, foundRepos)

	// Signal that we have no more entries by setting EOF
	if len(foundRepos) <= len(repos) {
		errVal = io.EOF
	}

	return n, errVal
}

// Update repositories in MySQL
func (reg *registry) UpdateRepositoriesSearchBackend(ctx context.Context) (n int, err error) {

	var foundRepos []string
	var errVal error

	root, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return 0, err
	}

	catalogRoot := "/catalog" + root
	children, err := reg.blobStore.driver.List(ctx, catalogRoot)
	if err != nil {
		return 0, err
	}

	for _, child := range children {
		repoPath := child[len(root) + 1:]
		foundRepos = append(foundRepos, repoPath)
	}

	sort.Strings(foundRepos)

	if os.Getenv("SEARCH_BACKEND") == "MYSQL" {
		DB := mysql.InitDatabase()

		if DB != nil {
			createSql := fmt.Sprintf("create table if not exists `repositories` ( `name` VARCHAR(1024) NOT NULL, `createTime` BIGINT(20) NOT NULL DEFAULT '0', PRIMARY KEY (`name`) ) COLLATE='utf8_general_ci' ENGINE=InnoDB")
			_, err = DB.Exec(createSql)
			if err != nil {
				log.Println("exec", createSql, "fail: ", err)
			}
			
			sql := fmt.Sprintf("delete from repositories")
			_, err = DB.Exec(sql)
			if err != nil {
				log.Println("exec", sql, "fail: ", err)
			}

			createTime := time.Now().Unix()
			for _, repo := range foundRepos {
				sql = fmt.Sprintf(
					"insert into repositories(name, createTime) values ('%s', %d) on duplicate key update createTime=%d",
					repo,
					createTime,
					createTime)
				_, err := DB.Exec(sql)
				if err != nil {
					log.Println("exec", sql, "fail: ", err)
				}
			}
			DB.Close()
		}
	} else if os.Getenv("SEARCH_BACKEND") == "LOCAL" {
		file, err := os.OpenFile("/opt/registry/repositories", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
		if err != nil {
			return len(foundRepos), err
		}
		writer := bufio.NewWriter(file)
		for _, repo := range foundRepos {
			_, err := writer.WriteString(repo + "\n")
			if err != nil {
				log.Println("write string fail: ", err)
			}
			writer.Flush()
		}
		file.Close()
	}

	return len(foundRepos), errVal
}

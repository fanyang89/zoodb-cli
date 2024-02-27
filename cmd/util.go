package cmd

import "os"

func fileExists(p string) (bool, error) {
	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func getDepth(key string) int {
	// / for 1
	// /zookeeper/config for 2
	c := 0
	for _, a := range key {
		if a == '/' {
			c++
		}
	}
	return c
}

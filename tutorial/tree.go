4ckage main

import (
	"fmt"

	"golang.org/x/tour/tree"
)

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *tree.Tree, ch chan int) {
	WalkAux(t, ch)
	close(ch)
}

func WalkAux(t *tree.Tree, ch chan int) {
	if t != nil {
		WalkAux(t.Left, ch)
		ch <- t.Value
		WalkAux(t.Right, ch)
	}
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {
	ch1 := make(chan int)
	ch2 := make(chan int)
	go Walk(t1, ch1)
	go Walk(t2, ch2)
	for {
		v1, s1 := <-ch1
		v2, s2 := <-ch2
		if s1 != s2 || v1 != v2 {
			return false
		}
		if s1 == false {
			break
		}

	}
	return true
}

func main() {
	fmt.Println(Same(tree.New(1), tree.New(2)))
	fmt.Println(Same(tree.New(1), tree.New(1)))
	fmt.Println(Same(tree.New(2), tree.New(2)))
}


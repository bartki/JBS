< x s l : s t y l e s h e e t   v e r s i o n = " 1 . 0 "   x m l n s : x s l = " h t t p : / / w w w . w 3 . o r g / 1 9 9 9 / X S L / T r a n s f o r m " >  
                                         < x s l : o u t p u t   m e t h o d = " x m l "   v e r s i o n = " 1 . 5 "   i n d e n t = " y e s "   o m i t - x m l - d e c l a r a t i o n = " n o "   / >  
                                         < x s l : s t r i p - s p a c e   e l e m e n t s = " * " / >  
                                         < x s l : t e m p l a t e   m a t c h = " n o d e ( ) | @ * " >  
                                               < x s l : c o p y >  
                                                     < x s l : a p p l y - t e m p l a t e s   s e l e c t = " n o d e ( ) | @ * " / >  
                                               < / x s l : c o p y >  
                                         < / x s l : t e m p l a t e >  
                                         < x s l : t e m p l a t e   m a t c h = " * [ n o t ( @ * | c o m m e n t ( ) | p r o c e s s i n g - i n s t r u c t i o n ( ) )   a n d   n o r m a l i z e - s p a c e ( ) = ' ' ] " / >  
                                         < x s l : t e m p l a t e   p r i o r i t y = " 2 "   m a t c h = " R O W " >  
                                               < W A R E H O U S E > < x s l : a p p l y - t e m p l a t e s / > < / W A R E H O U S E >  
                                         < / x s l : t e m p l a t e >  
                                         < x s l : t e m p l a t e   p r i o r i t y = " 2 "   m a t c h = " S T O C K S / S T O C K S _ R O W " >  
                                               < S T O C K > < x s l : a p p l y - t e m p l a t e s / > < / S T O C K >  
                                         < / x s l : t e m p l a t e >  
                                   < / x s l : s t y l e s h e e t > 